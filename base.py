  # base.py completo modificado
  
  import pandas as pd
  from sqlalchemy import create_engine
  import sqlalchemy as sa
  import numpy as np
  from datetime import datetime
  from dateutil.relativedelta import relativedelta
  import os
  
  # Configurações comuns de conexão
  azure_logon = os.environ['AZURE_LOGON']
  azure_password = os.environ['AZURE_PASSWORD']
  azure_host = os.environ['AZURE_HOST']
  my_port = os.environ['MY_PORT']
  my_odbc_driver = os.environ['MY_ODBC_DRIVER']
  
  # Função para criar engine de conexão
  def create_db_engine(database_name):
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
  
  def processar_dados(data_base):
      # Extrair mês e ano da data_base
      data_base_obj = datetime.strptime(data_base, '%Y-%m-%d')
      month_value = data_base_obj.month
      year_value = data_base_obj.year
      
      # Definir data_posicao como último dia do mês anterior
      data_posicao = (data_base_obj.replace(day=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
  
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
