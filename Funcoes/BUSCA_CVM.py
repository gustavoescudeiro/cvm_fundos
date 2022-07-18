import pandas as pd
import zipfile
import time
#pd.options.display.float_format = '{:.2f}'.format

from io import BytesIO
from zipfile import ZipFile
from urllib.request import urlopen


def busca_informes_cvm(ano, mes):
    url = 'http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{:02d}{:02d}.zip'.format(ano,mes)
    return pd.read_csv(url, sep=';')

def busca_cadastro_cvm():
    url = "http://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"
    return pd.read_csv(url, sep=';', encoding='ISO-8859-1')


def busca_informes_cvm_historico(ano):
    resp = urlopen(f"http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{ano}.zip")
    zipfile = ZipFile(BytesIO(resp.read()))
    lista_arquivos = zipfile.namelist()
    lista_df = []
    for arquivo in lista_arquivos:
        df = pd.read_csv(zipfile.open(arquivo), sep = ";", decimal = ",")
        lista_df.append(df)
    df_final = pd.concat(lista_df, axis = 0)
    return df_final

