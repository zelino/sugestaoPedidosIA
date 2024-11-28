import os
from sqlalchemy import create_engine
import pandas as pd
from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain import hub
from langchain_community.cache import SQLiteCache
from langchain_community.utilities import SQLDatabase
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_experimental.utilities import PythonREPL
from langchain.agents import create_react_agent, AgentExecutor, Tool
from langchain.prompts import PromptTemplate


## OPENAI
client_openai = ChatOpenAI(
    api_key=os.environ['OPENAI_API_KEY'],
    model="gpt-4o-mini",
    temperature=0.2,
    cache=SQLiteCache(database_path="cache.db")
)

## GROQ
client_groq = ChatGroq(
    api_key=os.environ['GROQ_API_KEY'],
    model_name="llama-3.1-70b-versatile",
    cache=SQLiteCache(database_path="cache.db")
)

# Set a higher limit for the number of cells that can be styled
pd.set_option("styler.render.max_elements", 7794324)


# Configurações de conexão
HOST = '10.5.5.22'
PORT = 1434
DATABASE = 'DW_Softec'
USER_BD = 'sa'
PASSWORD_BD = 'pm6e24X7Q^3x'
DRIVER = 'ODBC Driver 17 for SQL Server'


def get_connection():
    """Estabelece a conexão com o banco de dados."""
    engine = create_engine(f'mssql+pyodbc://{USER_BD}:{PASSWORD_BD}@{HOST}:{PORT}/{DATABASE}?driver={DRIVER}')
    return engine.connect()


def get_data_incremental(use_cache=True):
    """Obtém dados incrementais de vendas."""
    cache_file = 'dataset/data.csv'
    if use_cache and os.path.exists(cache_file):
        df_cache = pd.read_csv(cache_file, encoding='utf-8-sig', sep=';', low_memory=False)

        # Verifica se o DataFrame não está vazio
        if df_cache.empty:
            raise ValueError("O cache está vazio. Exclua o arquivo para recarregar os dados.")

        last_data_cache = pd.to_datetime(df_cache['Data'].max())

        # Query para buscar dados incrementais
        query = f"""
            SELECT * FROM VW_VENDAS_IA
            WHERE Data > '{last_data_cache.strftime('%Y-%m-%d')}'
            ORDER BY Data, Loja, Produto, Fornecedor
        """
        try:
            with get_connection() as conn:
                df_new_data = pd.read_sql(query, conn)
        except Exception as e:
            raise RuntimeError(f"Erro ao conectar no banco ou executar a consulta: {e}")

        # Adiciona novos dados se existirem
        if not df_new_data.empty:
            df_new_data.to_csv(cache_file, mode='a', header=False, index=False, encoding='utf-8-sig', sep=';')
            return pd.concat([df_cache, df_new_data], ignore_index=True)

        return df_cache

    # Caso não exista cache, retorna todos os dados
    query = 'SELECT * FROM VW_VENDAS_IA ORDER BY Data, Loja, Produto, Fornecedor'
    try:
        with get_connection() as conn:
            df = pd.read_sql(query, conn)
    except Exception as e:
        raise RuntimeError(f"Erro ao conectar no banco ou executar a consulta: {e}")

    df.to_csv(cache_file, index=False, encoding='utf-8-sig', sep=';')
    return df


def get_stock():
    """Obtém os dados de estoque."""
    query = '''
        SELECT 
            DE.produto_id,
            DD.agency_id AS Loja_ID,
            SUM(DE.total) AS Estoque_Total
        FROM dim_estoque DE
        LEFT JOIN dim_depositos DD ON DD.deposito_id = DE.deposito_id
        WHERE DD.agency_id != 0
        GROUP BY DE.produto_id, DD.agency_id
        ORDER BY DD.agency_id ASC
    '''
    try:
        with get_connection() as conn:
            df_stock = pd.read_sql(query, conn)
    except Exception as e:
        raise RuntimeError(f"Erro ao conectar no banco ou executar a consulta: {e}")

    # Validação do DataFrame
    if df_stock.empty:
        raise ValueError("Os dados de estoque retornaram vazios.")

    df_stock.to_csv('dataset/stock.csv',index=False, encoding='utf-8-sig', sep=';')
    return df_stock


def calculate_markup(df):
    """Calcula o markup."""
    df['Markup'] = df.apply(
        lambda row: (row['Preco_Unitario'] / row['Custo_Unitario'] - 1)
        if row['Custo_Unitario'] > 0 else None,
        axis=1
    )
    return df

def calculate_abc(df, percent_a, percent_b, base_column):
    df = df.copy()
    # Ordenar o DataFrame pelo Valor_Total_Venda em ordem decrescente
    df = df.sort_values(by=base_column, ascending=False)
    
    # Calcular o total de vendas
    total_vendas = df[base_column].sum()
    
    # Calcular o percentual acumulado de vendas
    df['Percentual Acumulado'] = (df[base_column].cumsum() / total_vendas) * 100
    
    df['Classe_ABC'] = 'C'  # Classe padrão
    df.loc[df['Percentual Acumulado'] <= percent_a * 100, 'Classe_ABC'] = 'A'
    df.loc[
        (df['Percentual Acumulado'] > percent_a * 100) & 
        (df['Percentual Acumulado'] <= percent_b * 100),
        'Classe_ABC'
    ] = 'B'
    
    return df

def calculate_stock_duration(df, total_dias):
    df['Qtd_Diaria_Media'] = df['Estoque_Total'] / total_dias
    df['Dias_Estoque'] = df['Quantidade_Vendida'] / df['Qtd_Diaria_Media']
    
    df['Dias_Estoque'] = df['Dias_Estoque'].replace([float('inf'), -float('inf')], 0)
    df['Dias_Estoque'] = df['Dias_Estoque'].fillna(0)
    df['Dias_Estoque'] = df['Dias_Estoque'].astype(int)
    
    return df

def project_stock(df, dias_a_projetar, total_dias):
    df['Projecao_Estoque'] = ((df['Quantidade_Vendida'] / total_dias) * dias_a_projetar) - df['Estoque_Total']
    df['Projecao_Estoque'] = df['Projecao_Estoque'].replace([float('inf'), -float('inf')], 0)
    df['Projecao_Estoque'] = df['Projecao_Estoque'].fillna(0)
    df['Projecao_Estoque'] = df['Projecao_Estoque'].astype(int)
    return df

def sell_cost(df):
    df['Custo_Venda'] = df['Quantidade_Vendida'] * df['Custo_Medio_Unitario']
    df['Custo_Venda'] = df['Custo_Venda'].replace([float('inf'), -float('inf')], 0)
    
    return df

def stock_cost(df):
    # df['Custo_Medio_Unitario'] = df['Custo_Medio_Unitario'].astype(int)
    df['Custo_Estoque'] =  df['Custo_Unitario'] * df['Estoque_Total']
    return df

def sell_frequency(df, period):
    df['Frequencia_Venda'] = df['Quantidade_Vendida'] / period
    df['Frequencia_Venda'] = df['Frequencia_Venda'].replace([float('inf'), -float('inf')], 0)
    df['Frequencia_Venda'] = df['Frequencia_Venda'].astype(int)
    return df

system_message = hub.pull("hwchase17/react")

def analisa_df_ia(df, prompt_template):
    # Criar o REPL do Python como ferramenta
    python_repl = PythonREPL()
    python_repl_tool = Tool(
        name="python_repl",
        description="Um shell Python. Use isso para executar comandos Python. "
                    "A entrada deve ser um comando Python válido. Para ver a saída de um valor, você deve imprimi-lo com `print(...)`.",
        func=python_repl.run,
    )
    
    # Criar o template do prompt
    prompt_template_df = PromptTemplate.from_template(prompt_template)
    
    # Criar o agente do DataFrame
    agent = create_pandas_dataframe_agent(
        llm=client_openai,  # Modelo LLM
        df=df,              # DataFrame fornecido
        prompt=prompt_template_df,
        agent_type="openai-functions",  # Ajustado para evitar conflitos
        verbose=True,
        tools=[python_repl_tool],  # Ferramentas incluídas diretamente no agente
        allow_dangerous_code=True  # Permite execução de código perigoso, como gerar gráficos
    )
    
    return agent

def style_curva_abc(val):
    color = 'green' if val == 'A' else '#D9B300' if val == 'B' else '#D64550' if val == 'C' else 'red'
    return f'background-color: {color}'
def style_stock_duration(val):
    color = 'red' if val <= 0 else 'green'
    return f'background-color: {color}'
def style_stock_projection(val):
    color = 'red' if val <= 0 else 'green'
    return f'background-color: {color}'

def format_pyg(value):
    return f"₲ {value:,.0f}".replace(",", ".")

def format_estoque(value):
    return f"{value:,.0f}".replace(",", ".")

def formart_markup(value):
    return f"{value:.2%}"

def format_gerenal(value):
    return f"{value:,.2f}".replace(", ", ".")

    
