from Funcoes.SQL_CON import Conn, Db_insert
import pandas as pd
from datetime import date
from Funcoes.BUSCA_CVM import busca_informes_cvm
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

conexao = Conn()
# Verificando ultimo mes na base
query = """select max (dt_comptc) from dados_fundos"""
df = pd.read_sql(query, conexao)
date_max = df["max"].iloc[0]

# date_max eh a ultima data no banco de dados

current_year = date.today().year
last_month_year = f"""{str(current_year)}-12-01"""

list_dates = pd.date_range(date_max,last_month_year,freq='M').strftime("%Y-%m").tolist()


# carregando dfs
lista_recente = []
for date in list_dates:
    try:
        df = busca_informes_cvm(int(date.split("-")[0]), int(date.split("-")[1]))

        # Ajustando colunas
        colunas_ajustadas = [x.lower() for x in df.columns]
        df.columns = colunas_ajustadas
        dict_de_para_cols = {'cnpj_fundo_classe': 'cnpj_fundo',
                             'tp_fundo_classe': 'tp_fundo'}
        df.rename(columns=dict_de_para_cols, inplace=True)

        # Identificando colunas da tabela
        query_colunas = """SELECT
            column_name,
            data_type
        FROM
            information_schema.columns
        WHERE
            table_name = 'dados_fundos'"""
        df_columns_info = pd.read_sql(query_colunas, conexao)
        lista_colunas_tabela = df_columns_info['column_name'].to_list()

        # Filtrando colunas
        df_final = df[lista_colunas_tabela]

        # Dropando possiveis duplicados
        df_final.drop_duplicates(subset=["cnpj_fundo", "dt_comptc"], inplace=True)
        list_dates = df_final["dt_comptc"].unique()
        dates = (', '.join("""'""" + item + """'""" for item in list_dates))

        # Inserindo dados
        try:
            # Deletando antes de inserir
            cur = conexao.cursor()
            sql_delete = f"""DELETE FROM DADOS_FUNDOS WHERE dt_comptc in ({dates}) """
            cur.execute(sql_delete)
            Db_insert(df=df_final, tabela="dados_fundos")
            print("OK")
            print("Dados inseridos")
        except Exception as e:
            print(e)

        print(f"arquivo para data {date} carregado")
    except Exception as e:
        print(e)
        continue





