from io import BytesIO
from zipfile import ZipFile

import os
import requests
'''
baixa dados do tcsp e siscomex e cria diretórios para armazená-los
'''


def fetch_receita():
    # hardcoded para tcsp anos
    receita_anos = list(range(2008, 2024))

    print(receita_anos)
    for ano in receita_anos:
        url = f"https://transparencia.tce.sp.gov.br/sites/default/files/conjunto-dados/receitas-{ano}.zip"
        resp = requests.get(url)
        zipfile = ZipFile(BytesIO(resp.content))
        zipfile.extractall('./datalake/receitas')


def fetch_siscomex():
    # hardcoded para siscomex
    anos = list(range(1997, 2024))

    for ano in anos:
        for imp_exp in ['IMP', 'EXP']:
            # url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{imp_exp}_{ano}.csv"
            url = f"https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/{imp_exp}_{ano}_MUN.csv"
            resp = requests.get(url, verify=False)
            if resp.status_code != 200:
                print('download error', resp.status_code)
                break

            if imp_exp == 'IMP':
                with open(f'./datalake/importacao_mun/{ano}.csv', 'wb') as fp:
                    fp.write(resp.content)

            elif imp_exp == 'EXP':
                with open(f'./datalake/exportacao_mun/{ano}.csv', 'wb') as fp:
                    fp.write(resp.content)


if __name__ == '__main__':
    # criar diretórios se não existirem
    data_lake_dir = './datalake'
    importacao_dir = 'importacao_mun'
    exportacao_dir = 'exportacao_mun'
    receitas_dir = 'receitas'

    if not os.path.exists(data_lake_dir):
        os.mkdir(data_lake_dir)
    if not os.path.exists(f'{data_lake_dir}/{importacao_dir}'):
        os.mkdir(f'{data_lake_dir}/{importacao_dir}')
    if not os.path.exists(f'{data_lake_dir}/{exportacao_dir}'):
        os.mkdir(f'{data_lake_dir}/{exportacao_dir}')
    if not os.path.exists(f'{data_lake_dir}/{receitas_dir}'):
        os.mkdir(f'{data_lake_dir}/{receitas_dir}')

    fetch_receita()
    fetch_siscomex()
