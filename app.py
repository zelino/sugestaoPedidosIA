import streamlit as st
from streamlit_card import card
import numpy as np
import pandas as pd
import pandas as pd
import io
import xlsxwriter
from functions import (
    calculate_markup, 
    get_stock, 
    get_data_incremental, 
    format_pyg, 
    format_estoque, 
    formart_markup, 
    calculate_abc, 
    style_curva_abc,
    calculate_stock_duration,
    style_stock_duration,
    project_stock,
    style_stock_projection,
    sell_cost,
    stock_cost,
    analisa_df_ia,
    sell_frequency,
    format_estoque
    )

# Configuração inicial do Streamlit
st.set_page_config(
    layout="wide",
    page_title="Gerador de pedidos",
    initial_sidebar_state='auto'
)

# Carregar os dados na sessão
if "data" not in st.session_state:
    df_data = get_data_incremental()
    df_data = pd.read_csv('dataset/data.csv', encoding='utf-8-sig', sep=';', low_memory=False)
    st.session_state["data"] = df_data

if "stock" not in st.session_state:
    df_stock = get_stock()
    df_stock = pd.read_csv('dataset/stock.csv', encoding='utf-8-sig', sep=';')
    st.session_state["stock"] = df_stock

# Trabalhar com os dados na sessão
df_data = st.session_state["data"]
df_stock = st.session_state["stock"]

df_data = df_data.astype({
    "Loja_ID": str,
    "produto_id": str
})
df_stock = df_stock.astype({
    "Loja_ID": str,
    "produto_id": str
})

# df_data = pd.to_datetime(df_data['Data'], format='%Y-%m-%d')
st.markdown("# Gerador Curva ABC")
with st.sidebar:
    st.markdown("# Menu")
    st.spinner("Gerando")
    # Formatar a coluna de datas
    df_data['Data'] = pd.to_datetime(df_data['Data'], format='%Y-%m-%d')
    min_data = df_data['Data'].min().date()
    max_data = df_data['Data'].max().date()

    # Filtros de período
    data_inicio = st.sidebar.date_input(
        'Escolha uma data inicial',
        min_value=min_data,
        max_value=max_data,
        format='DD/MM/YYYY'
    )

    data_fim = st.sidebar.date_input(
        'Escolha uma data final',
        min_value=data_inicio,
        max_value=max_data,
        format='DD/MM/YYYY'
    )

    if data_inicio is None or data_fim is None:
        st.error("Por favor, selecione um período válido.")
        st.stop()

    lojas = st.sidebar.multiselect(
        "Lojas",
        options=df_data['Loja'].unique()
    )
    fornecedores = st.sidebar.multiselect(
        "Fornecedores",
        options=df_data['Fornecedor'].unique()
    )
    grupo = st.multiselect(
        "Grupo",
        options=df_data['Grupo'].unique()
    )

    criterio_abc = st.sidebar.radio(
        "Escolha o critério para a curva ABC:",
        options=["Valor", "Quantidade"]
    )
    coluna_base = "Valor_Total_Venda" if criterio_abc == "Valor" else "Quantidade_Vendida"

    # Slider para Curva A
    curva_A_percent = st.sidebar.slider('Curva A (%)', 0, 100, 30)

    # Slider para Curva B com restrição
    curva_B_percent = st.sidebar.slider('Curva B (%)', 0, 100, 30)

    curva_A = curva_A_percent / 100
    curva_B = (curva_A_percent + curva_B_percent) / 100
    
    total_dias = (data_fim - data_inicio).days
    dias_a_projetar = st.sidebar.number_input('Dias a projetar', min_value=1, value=30)

df_filtrado = df_data[
    (df_data['Loja'].isin(lojas) if lojas else True) &
    (df_data['Fornecedor'].isin(fornecedores) if fornecedores else True) &
    (df_data['Grupo'].isin(grupo) if grupo else True) &
    (df_data['Data'] >= pd.to_datetime(data_inicio)) &
    (df_data['Data'] <= pd.to_datetime(data_fim))
]

vendas_agrupadas = df_filtrado.groupby(['produto_id', 'Produto', 'Referencia', 'Grupo']).agg(
    Quantidade_Vendida=('Quantidade_Vendida', 'sum'),
    Valor_Total_Venda=('Valor_Total_Venda', 'sum'),
    Preco_Medio_Unitario=('Preco_Medio_Unitario', 'first'),
    Custo_Medio_Unitario=('Custo_Medio_Unitario', 'first'),
    Custo_Unitario=('Custo_Unitario', 'first'),
    Preco_Unitario=('Preco_Unitario', 'first'),
).reset_index()

if lojas:
    loja_ids_selecionados = df_data.loc[df_data["Loja"].isin(lojas), "Loja_ID"].unique()
else:
    loja_ids_selecionados = df_stock["Loja_ID"].unique()

estoque_filtrado = df_stock[
    df_stock["Loja_ID"].isin(loja_ids_selecionados)
]

# Realizar o merge entre vendas e estoque
resultado = pd.merge(
    vendas_agrupadas,
    estoque_filtrado[["produto_id", "Estoque_Total"]].groupby("produto_id").sum().reset_index(),
    on="produto_id",
    how="left"
).fillna({"Estoque_Total": 0})

resultado = calculate_markup(resultado)
resultado = calculate_abc(resultado, curva_A, curva_B, coluna_base)
resultado = resultado.drop(columns=['Percentual Acumulado'])
resultado = calculate_stock_duration(resultado, total_dias)
resultado = resultado.drop(columns=['Qtd_Diaria_Media'])
resultado = project_stock(resultado, dias_a_projetar, total_dias)
resultado = sell_cost(resultado)
resultado = sell_frequency(resultado, total_dias)

resultado = stock_cost(resultado)

valor_acumulado = format_pyg(resultado[coluna_base].sum())
qtd_acumulado = format_estoque(resultado['Quantidade_Vendida'].sum())

resultado = resultado.rename(columns={
    'produto_id': 'Produto_ID',
    'Produto': 'Produto',
    'Quantidade_Vendida': 'Qtd. Vendida',
    'Valor_Total_Venda': 'Valor Vendido',
    'Preco_Unitario': 'Preço Un.',
    'Custo_Unitario': 'Custo Un.',
    'Estoque_Total' : 'Estoque Total',
    'Markup': 'Markup',
    'Dias_Estoque': 'Dias de Estoque',
    'Classe_ABC': 'Curva',
    'Projecao_Estoque': 'Proj. Estoque',
    'Custo_Venda': 'Custo de Venda',
    'Custo_Estoque': 'Custo do Estoque',
    'Frequencia_Venda': 'Freq. Venda'
})

new_order = [
    'Produto_ID',
    'Referencia', 
    'Produto', 
    'Curva', 
    'Valor Vendido',
    'Qtd. Vendida',
    'Freq. Venda',
    'Custo de Venda',
    'Estoque Total', 
    'Dias de Estoque',
    'Custo do Estoque',
    'Custo Un.', 
    'Preço Un.', 
    'Markup',
    'Proj. Estoque'
    ]

resultado = resultado[new_order]

styled_result = resultado.style.applymap(style_curva_abc, subset=['Curva'])
styled_result = styled_result.applymap(style_stock_duration, subset=['Dias de Estoque'])
styled_result = styled_result.applymap(style_stock_projection, subset=['Proj. Estoque'])


# Divisão em 3 colunas
col1, col2, col3, col4 = st.columns(4)

def card_curva_a():
    venda_curva_a = format_pyg(resultado.loc[resultado['Curva'] == 'A', 'Valor Vendido'].sum())
    custo_venda_curva_a = format_pyg(resultado.loc[resultado['Curva'] == 'A', 'Custo de Venda'].sum())
    custo_estoque_curva_a = format_pyg(resultado.loc[resultado['Curva'] == 'A', 'Custo do Estoque'].sum())
    
    venda_curva_a_float = resultado.loc[resultado['Curva'] == 'A', 'Valor Vendido'].sum()
    custo_venda_curva_a_float = resultado.loc[resultado['Curva'] == 'A', 'Custo de Venda'].sum()

    # Verificando se o custo de venda não é zero ou NaN antes de calcular o markup
    if custo_venda_curva_a_float != 0 and not np.isnan(custo_venda_curva_a_float):
        markup_curva_a = ((venda_curva_a_float - custo_venda_curva_a_float) / custo_venda_curva_a_float) * 100
    else:
        markup_curva_a = 0  # Caso o custo de venda seja zero ou NaN, o markup será zero
    
    # Formatação do Markup como string com percentual
    markup_curva_a_str = f"{markup_curva_a:,.2f}%"
    
    card(
        title="Curva A",
        text = [
            "Valor de venda" ,venda_curva_a,
            "______________________________",
            "Custo da venda", custo_venda_curva_a,
            "______________________________",
            "Custo do estoque", custo_estoque_curva_a,
            "______________________________",
            "Markup", markup_curva_a_str
            ],
        styles={
        "card": {
            "width": "100%",
            "height": "300px",
            "background-color": "#f0f0f0",
            "border-radius": "8px",
            "padding": "20px",
            "box-shadow": "0 4px 6px rgba(0, 0, 0, 0.1)",
            "text-align": "center",
            "font-size": "14px",
            "border-radius": "4px"
            }
        }
    )
    
def card_curva_b():
    venda_curva_b = format_pyg(resultado.loc[resultado['Curva'] == 'B', 'Valor Vendido'].sum())
    custo_venda_curva_b = format_pyg(resultado.loc[resultado['Curva'] == 'B', 'Custo de Venda'].sum())
    custo_estoque_curva_b = format_pyg(resultado.loc[resultado['Curva'] == 'B', 'Custo do Estoque'].sum())
    
    venda_curva_b_float = resultado.loc[resultado['Curva'] == 'B', 'Valor Vendido'].sum()
    custo_venda_curva_b_float = resultado.loc[resultado['Curva'] == 'B', 'Custo de Venda'].sum()

    # Verificando se o custo de venda não é zero ou NaN antes de calcular o markup
    if custo_venda_curva_b_float != 0 and not np.isnan(custo_venda_curva_b_float):
        markup_curva_b = ((venda_curva_b_float - custo_venda_curva_b_float) / custo_venda_curva_b_float) * 100
    else:
        markup_curva_b = 0  # Caso o custo de venda seja zero ou NaN, o markup será zero
    
    # Formatação do Markup como string com percentual
    markup_curva_b_str = f"{markup_curva_b:,.2f}%"
    
    card(
        title="Curva B",
        text = [
            "Valor de venda" ,venda_curva_b,
            "______________________________",
            "Custo da venda", custo_venda_curva_b,
            "______________________________",
            "Custo do estoque", custo_estoque_curva_b,
            "______________________________",
            "Markup", markup_curva_b_str
            ],
        styles={
        "card": {
            "width": "100%",
            "height": "300px",
            "background-color": "#f0f0f0",
            "border-radius": "8px",
            "padding": "20px",
            "box-shadow": "0 4px 6px rgba(0, 0, 0, 0.1)",
            "text-align": "center",
            "font-size": "14px",
            "border-radius": "4px"
            }
        }
    )

def card_curva_c():
    venda_curva_c = format_pyg(resultado.loc[resultado['Curva'] == 'C', 'Valor Vendido'].sum())
    custo_venda_curva_c = format_pyg(resultado.loc[resultado['Curva'] == 'C', 'Custo de Venda'].sum())
    custo_estoque_curva_c = format_pyg(resultado.loc[resultado['Curva'] == 'C', 'Custo do Estoque'].sum())
    
    venda_curva_c_float = resultado.loc[resultado['Curva'] == 'C', 'Valor Vendido'].sum()
    custo_venda_curva_c_float = resultado.loc[resultado['Curva'] == 'C', 'Custo de Venda'].sum()

    # Verificando se o custo de venda não é zero ou NaN antes de calcular o markup
    if custo_venda_curva_c_float != 0 and not np.isnan(custo_venda_curva_c_float):
        markup_curva_c = ((venda_curva_c_float - custo_venda_curva_c_float) / custo_venda_curva_c_float) * 100
    else:
        markup_curva_c = 0  # Caso o custo de venda seja zero ou NaN, o markup será zero
    
    # Formatação do Markup como string com percentual
    markup_curva_c_str = f"{markup_curva_c:,.2f}%"
    
    card(
        title="Curva C",
        text = [
            "Valor de venda" ,venda_curva_c,
            "______________________________",
            "Custo da venda", custo_venda_curva_c,
            "______________________________",
            "Custo do estoque", custo_estoque_curva_c,
            "______________________________",
            "Markup", markup_curva_c_str
            ],
        styles={
        "card": {
            "width": "100%",
            "height": "300px",
            "background-color": "#f0f0f0",
            "border-radius": "8px",
            "padding": "20px",
            "box-shadow": "0 4px 6px rgba(0, 0, 0, 0.1)",
            "text-align": "center",
            "font-size": "14px",
            "border-radius": "4px"
            }
        }
    )
    
def card_curva_acumulado():
    venda_curva_acumulado = format_pyg(resultado['Valor Vendido'].sum())
    custo_venda_curva_acumulado = format_pyg(resultado['Custo de Venda'].sum())
    custo_estoque_curva_acumulado = format_pyg(resultado['Custo do Estoque'].sum())
    
    venda_curva_acumulado_float = resultado['Valor Vendido'].sum()
    custo_venda_curva_acumulado_float = resultado['Custo de Venda'].sum()

    # Verificando se o custo de venda não é zero ou NaN antes de calcular o markup
    if custo_venda_curva_acumulado_float != 0 and not np.isnan(custo_venda_curva_acumulado_float):
        markup_curva_acumulado = ((venda_curva_acumulado_float - custo_venda_curva_acumulado_float) / custo_venda_curva_acumulado_float) * 100
    else:
        markup_curva_acumulado = 0  # Caso o custo de venda seja zero ou NaN, o markup será zero
    
    # Formatação do Markup como string com percentual
    markup_curva_acumulado_str = f"{markup_curva_acumulado:,.2f}%"
    
    card(
        title="Acumulado",
        text = [
            "Valor de venda" ,venda_curva_acumulado,
            "______________________________",
            "Custo da venda", custo_venda_curva_acumulado,
            "______________________________",
            "Custo do estoque", custo_estoque_curva_acumulado,
            "______________________________",
            "Markup", markup_curva_acumulado_str
            ],
        styles={
        "card": {
            "width": "100%",
            "height": "300px",
            "background-color": "#f0f0f0",
            "border-radius": "8px",
            "padding": "20px",
            "box-shadow": "0 4px 6px rgba(0, 0, 0, 0.1)",
            "text-align": "center",
            "font-size": "14px",
            "border-radius": "4px"
            }
        }
    )

# Coluna 1: Curva A
with col1:
    card_curva_a()

# Coluna 2: Curva B
with col2:
    card_curva_b()

# Coluna 3: Curva C
with col3:
    card_curva_c()
    
with col4:
    card_curva_acumulado()

# Exibir a tabela no Streamlit
st.subheader("Resumo por Produto")
st.dataframe(styled_result.format({
    'Valor Vendido': format_pyg,
    'Qtd. Vendida': format_estoque,
    'Preço Un.': format_pyg,
    'Custo Un.': format_pyg,
    'Estoque Total': format_estoque,
    'Markup': formart_markup,
    'Proj. Estoque': format_estoque,
    'Custo de Venda': format_pyg,
    'Custo do Estoque': format_pyg,
    'Freq. Venda': format_estoque
}),use_container_width=True, height=600)
abc = io.BytesIO()

# Salvando o DataFrame no arquivo Excel
with pd.ExcelWriter(abc, engine='xlsxwriter') as writer:
    styled_result.to_excel(writer, index=False, sheet_name='Curva ABC')

# Recuperando o conteúdo do arquivo Excel gerado
abc.seek(0)  # Move o ponteiro de leitura de volta ao início do arquivo

# Exibindo o botão para download do Excel
st.download_button(
    label="📥 Baixar como Excel",
    data=abc,
    file_name="curvaABC.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
                    
with st.expander("Análise de Dados"):
    # Botão para acionar a análise
    if st.button("Gerar Análise"):
        with st.spinner("Analisando dados..."):
            try:
                # Definir o template do prompt para análise
                prompt_df = f"""
                    Você é um especialista em análise de dados com ampla experiência em vendas, gestão de estoque e reposição de produtos. 
                    Sua tarefa é analisar integralmente o DataFrame fornecido, avaliando todos os produtos sem exceção.
                    A moeda utilizada é o Guarani (Gs), e todo valor deve ser separado por ponto e não por vírgula.
                    Com base nos dados, identifique e forneça recomendações detalhadas para:
                        Reposição de estoque: Identifique produtos que precisam ser repostos, garantindo a análise completa de todos os itens.
                        Otimização de estoque: Identifique produtos com baixa frequência de venda e estoque excessivo, e proponha estratégias para aumentar sua movimentação ou ajustar seu estoque.
                    Critérios de análise:
                        Utilize as colunas disponíveis:
                        Freq. Venda: frequência média de vendas diárias.
                        Estoque Total: quantidade disponível atualmente.
                        Custo Un.: custo unitário para reposição.
                        Outras colunas relevantes relacionadas às tendências de vendas, se disponíveis.
                    Reposição de estoque:
                        Para os produtos sugeridos para reposição, calcule e leve em consideração:
                            A quantidade necessária para atender à demanda projetada dos próximos {dias_a_projetar} dias.
                            Estoque atual suficiente para cobrir essa demanda dispensa a reposição.
                            Tendências observadas, como sazonalidade, picos ou quedas recentes nas vendas.
                    Otimização de estoque:
                        Identifique itens com estoque elevado em relação à demanda, sugerindo estratégias como:
                            Promoções.
                            Ajustes no volume de reposição futura.
                            Alterações na estratégia de vendas.
                    Resultados esperados:
                        Produtos que necessitam de reposição:
                            Nome do produto.
                            Estoque atual.
                            Frequência média de venda diária.
                            Quantidade sugerida para reposição (baseada na demanda projetada para {dias_a_projetar} dias).
                            Custo total da reposição (Custo Un. × quantidade sugerida).
                            Motivo da reposição (ex.: alta demanda, baixa cobertura, tendências observadas).
                    Produtos com estoque excessivo:
                        Nome do produto.
                        Estoque atual.
                        Frequência média de venda diária.
                        Sugestões para otimização (ex.: promoções, ajustes no volume de compras futuras).
                        
                        Formato de apresentação:
                            Apresente os resultados em dois DataFrames organizados:
                                Um para os produtos que necessitam de reposição, com as colunas solicitadas.
                                Outro para os produtos com estoque excessivo, detalhando as sugestões de otimização.
                            Inclua uma análise geral da saúde do estoque atual, destacando:
                                Com base no estoque e nas vendas, identifique produtos com alto risco de ruptura.
                                Tendências gerais observadas (ex.: categorias com maior demanda, sazonalidades).
                            Produtos com Risco de Ruptura:
                        Detalhe quais produtos estão com esse risco e medidas preventivas.

                    Apresente a resposta no formato de um DataFrame para facilitar a exibição.
                """

                # Criar o agente de análise
                agent = analisa_df_ia(resultado, prompt_df)

                # Obter a resposta da IA
                response = agent.run(prompt_df)

                # Exibir os insights da IA
                st.markdown("### Resposta da IA")
                st.markdown(response)

                # Converter a resposta da IA para DataFrame
                sugestoes_reposicao_prompt = f"""
                    Gere um csv com os produtos da primeira resposta {response} separados por ; gere um arquivo com nome produtos_reposicao.csv, 
                    se o arquivo já existir pode sobrescrever com os seguintes campos:
                    - Id do Produto
                    - Nome do Produto
                    - Estoque Atual
                    - Custo Un.
                    - Quantidade Sugerida para Reposição
                """
                agent.run(sugestoes_reposicao_prompt)

                # Tentar carregar o CSV gerado
                try:
                    df_sugestoes = pd.read_csv('produtos_reposicao.csv', sep=';', encoding='utf-8-sig')
                except Exception:
                    df_sugestoes = pd.DataFrame()  # DataFrame vazio em caso de erro

                # Exibir os produtos sugeridos para reposição
                st.markdown("### Produtos Sugeridos para Reposição")
                if df_sugestoes.empty:
                    st.write("Nenhum produto precisa de reposição no momento.")
                else:
                    st.dataframe(
                        df_sugestoes.style.format({
                            'Estoque Atual': format_estoque,
                            'Freq. Venda': format_estoque,
                            'Quantidade Sugerida para Reposição': format_estoque
                        }),
                        use_container_width=True
                    )
                    output = io.BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df_sugestoes.to_excel(writer, index=False, sheet_name='Sugestoes')
                    excel_data = output.getvalue()
                    
                    st.download_button(
                        label="📥 Baixar como Excel",
                        data=excel_data,
                        file_name="sugestoes_reposicao.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

            except Exception as e:
                st.error(f"Erro ao gerar análise: {e}")





