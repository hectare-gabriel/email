# base.py completo modificado

import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sa
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

# Configurações comuns de conexão
azure_logon = os.environ['AZURE_LOGON']
azure_password = os.environ['AZURE_PASSWORD']
azure_host = os.environ['AZURE_HOST']
my_port = os.environ['MY_PORT']
my_odbc_driver = os.environ['MY_ODBC_DRIVER']


def create_db_engine(database_name):
    """Cria engine de conexão com o banco de dados especificado"""
    connection_uri = sa.engine.url.URL.create(
        "mssql+pyodbc",
        username=azure_logon,
        password=azure_password,
        host=azure_host,
        port=my_port,
        database=database_name,
        query={"driver": my_odbc_driver},
    )
    return create_engine(connection_uri)


# Criar engines para ambos os bancos de dados
engine_inputs = create_db_engine("HECTARE_INPUTS")
engine_reports = create_db_engine("HECTARE_REPORTS")


def calcular_data_posicao(data_base_str):
    """
    Calcula a data de posição (d-2 dias úteis)
    
    Args:
        data_base_str (str): Data base no formato 'YYYY-MM-DD'
        
    Returns:
        str: Data de posição no formato 'YYYY-MM-DD'
    """
    data_base = datetime.strptime(data_base_str, '%Y-%m-%d')
    dias_subtraidos = 0
    data_posicao = data_base
    
    while dias_subtraidos < 2:
        data_posicao -= timedelta(days=1)
        if data_posicao.weekday() < 5:  # 0=segunda, 4=sexta
            dias_subtraidos += 1
    
    return data_posicao.strftime('%Y-%m-%d')


def processar_dados(data_base=None):
    """
    Processa os dados para a data base especificada ou usa variável de ambiente
    
    Args:
        data_base (str, optional): Data base no formato 'YYYY-MM-DD'. 
                                 Se None, usa variável de ambiente ou calcula d-2 dias úteis.
                                 
    Returns:
        tuple: DataFrames (df_vortx, df_pl, df_fundos) contendo os dados processados
    """
    # Determinar a data base (prioridade: parâmetro > variável de ambiente > data atual -2 dias úteis)
    if data_base is None:
        data_base = os.getenv('DATA_BASE')
        if data_base is None:
            # Default local: d-2 dias úteis da data atual
            hoje = datetime.now()
            dias_subtraidos = 0
            data_base_calc = hoje
            
            while dias_subtraidos < 2:
                data_base_calc -= timedelta(days=1)
                if data_base_calc.weekday() < 5:
                    dias_subtraidos += 1
            
            data_base = data_base_calc.strftime('%Y-%m-%d')
    
    data_posicao = calcular_data_posicao(data_base)
    
    # Extrair mês e ano da data_posicao
    data_posicao_obj = datetime.strptime(data_posicao, '%Y-%m-%d')
    month_value = data_posicao_obj.month
    year_value = data_posicao_obj.year

    ####################################################
    # df_vortx
    ####################################################
    query_vortx = f"""
        SELECT v.*, c.nome, c.id_operacao, o.nome_op
        FROM [Vortx.RendaFixa] v
        LEFT JOIN [dbo].[ft_cadastro] c ON v.CodigoCustodia = c.codigo
        LEFT JOIN [dbo].[yd_operacao] o ON c.id_operacao = o.id_operacao
        WHERE v.Tipo = 'CRI'
        AND v.DataPosicao = '{data_base}'
        AND v.Carteira in (30248180, 1320)
    """
    df_vortx = pd.read_sql(query_vortx, engine_reports)

    # Renomear valores da coluna Carteira no df_vortx (versão segura para tipos)
    df_vortx['Carteira'] = df_vortx['Carteira'].astype(str).str.strip().replace({
        '1320': 'HCHG',
        '30248180': 'HCTR',
    })

    ####################################################
    # df_pl
    ####################################################
    query_pl = f"""
        SELECT *
        FROM [dbo].[Vortx.InfoGerais]
        WHERE DataPosicao = '{data_base}'
    """
    df_pl = pd.read_sql(query_pl, engine_reports)

    # Renomear valores da coluna Carteira no df_pl (se existir)
    if 'Carteira' in df_pl.columns:
        df_pl['Carteira'] = df_pl['Carteira'].astype(str).str.strip().replace({
            '1320': 'HCHG',
            '30248180': 'HCTR',
        })
    
    ####################################################
    # df_fundos - Combinação de RendaVariavel e CotasAplicadas
    ####################################################
    # Query para RendaVariavel
    query_renda_variavel = f"""
        SELECT 
            DataPosicao,
            Carteira,
            Titulo,
            MercadoAtual
        FROM [dbo].[Vortx.RendaVariavel]
        WHERE DataPosicao = '{data_base}'
        AND Carteira = '30248180'
    """
    
    # Query para CotasAplicadas (usa ValorLiquido mas vamos renomear)
    query_cotas_aplicadas = f"""
        SELECT 
            DataPosicao,
            Carteira,
            Titulo,
            ValorLiquido AS MercadoAtual
        FROM [dbo].[Vortx.CotasAplicadas]
        WHERE DataPosicao = '{data_base}'
        AND Carteira = '30248180'
        AND Titulo IN ('FII LOTEAMEN', 'SERRA VER')
    """
    
    # Ler os dataframes separadamente
    df_renda_variavel = pd.read_sql(query_renda_variavel, engine_reports)
    df_cotas_aplicadas = pd.read_sql(query_cotas_aplicadas, engine_reports)
    
    # Concatenar verticalmente
    df_fundos = pd.concat([df_renda_variavel, df_cotas_aplicadas], axis=0, ignore_index=True)
    
    # Renomear valores da coluna Carteira (se existir)
    if 'Carteira' in df_fundos.columns:
        df_fundos['Carteira'] = df_fundos['Carteira'].astype(str).str.strip().replace({
            '30248180': 'HCTR',
        })
    
    # Retornar os DataFrames processados (agora incluindo df_fundos)
    return df_vortx, df_pl, df_fundos