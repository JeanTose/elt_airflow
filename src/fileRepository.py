import psycopg2
from psycopg2 import sql, extras
import numpy as np
import pendulum
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


def connectBdd():
    hook = PostgresHook(postgres_conn_id='proj_etl')
    conn = hook.get_conn()
    return conn

    #cur = conn.cursor()
    #cur.execute(query)
    #conn.commit()

def create_table(conn):
    create = '''
    CREATE TABLE IF NOT EXISTS btres (
        tipo_registro INT,
        data_pregao DATE,
        cod_bdi NUMERIC,
        cod_negociacao VARCHAR(255),
        tipo_mercado INT,
        nome_empresa VARCHAR(255),
        especificacao_papel VARCHAR(255),
        prazo_dias_merc_termo NUMERIC,
        moeda_referencia VARCHAR(255),
        preco_abertura NUMERIC,
        preco_maximo NUMERIC,
        preco_minimo NUMERIC,
        preco_medio NUMERIC,
        preco_ultimo_negocio NUMERIC,
        preco_melhor_oferta_compra NUMERIC,
        preco_melhor_oferta_venda NUMERIC,
        numero_negocios NUMERIC,
        quantidade_papeis_negociados NUMERIC,
        volume_total_negociado NUMERIC,
        preco_exercicio NUMERIC,
        indicador_correcao_precos NUMERIC,
        data_vencimento NUMERIC,
        fator_cotacao NUMERIC,
        preco_exercicio_pontos NUMERIC,
        codigo_isin VARCHAR(20),
        num_distribuicao_papel NUMERIC
    );
    '''
    conn.cursor().execute(create)
    conn.commit()

def drop_table(conn):
    delete = '''
    DROP TABLE IF EXISTS btres;
    '''

    
    conn.cursor().execute(delete)
    conn.commit()

def insert_data(conn, table_name, dataframe):
    """
    Função para inserir dados em uma tabela no banco de dados.
    """
    # Converte a coluna "data_pregao" para o tipo de dado correto (DATE)
    dataframe["data_pregao"] = pd.to_datetime(dataframe["data_pregao"], format='%Y%m%d', errors='coerce')

    # Preenche valores nulos na coluna "data_pregao" com uma data padrão (você pode ajustar isso conforme necessário)
    dataframe["data_pregao"].fillna(pd.to_datetime('19000101', format='%Y%m%d'), inplace=True)

    valid_columns = [col for col in dataframe.columns]
    data = [tuple(row[column] for column in valid_columns) for _, row in dataframe.iterrows()]

    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s".format(table_name, ', '.join(valid_columns)))

    extras.execute_values(conn.cursor(), insert_query, data, template=None, page_size=100)

    conn.commit()