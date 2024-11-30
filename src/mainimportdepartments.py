import requests
import mysql.connector
from configparser import ConfigParser
from datetime import datetime
import json
import traceback
import time

# Função para carregar as configurações do arquivo ini
def carregar_configuracoes(cursor, ini_file='config-departments.ini'):
    config = ConfigParser()
    config.read(ini_file)
    
    pagina_inicial = int(config['API'].get('page', 0))
    per_page = int(config['API'].get('perPage', 3))  # Valor padrão é 3
    bearer_token = config['API'].get('bearerToken')

    sql = """
    DELETE FROM digisac_departments WHERE current_page >= (%s);
    """
    data = (str(pagina_inicial),)
    cursor.execute(sql, data)

    return pagina_inicial, per_page, bearer_token

# Função para criar conexão com o banco de dados MySQL
def criar_conexao_mysql():
    return mysql.connector.connect(
        host='129.148.44.247',  # Altere conforme necessário
        user='dbempresas',       # Altere conforme necessário
        password=']@Ym"iH4\\%s&ijXF',  # Altere conforme necessário
        database='dbempresas'  # Altere conforme necessário
    )

# Função para formatar data
def format_datetime(date_str):
    if date_str:
        return datetime.strptime(date_str[:-1], '%Y-%m-%dT%H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
    return None

# Função para inserir os dados no banco de dados
def inserir_dados_mysql(cursor, item, pagination_info):
    sql = """
    INSERT INTO digisac_departments (
        id, name, archived_at, created_at, updated_at, 
        account_id, distribution_id, total, `limit`, 
        skip, current_page, last_page, `from`, `to`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare data for insertion
    data = (
        item.get('id'),
        item.get('name'),
        format_datetime(item.get('archivedAt')),
        format_datetime(item.get('createdAt')),
        format_datetime(item.get('updatedAt')),
        item.get('accountId'),
        item.get('distributionId'),
        pagination_info['total'],
        pagination_info['limit'],
        pagination_info['skip'],
        pagination_info['currentPage'],
        pagination_info['lastPage'],
        pagination_info['from'],
        pagination_info['to']
    )

    cursor.execute(sql, data)

# Função principal para processar as páginas
def processar_paginas(ini_file='config-departments.ini'):
    conexao = criar_conexao_mysql()
    cursor = conexao.cursor()
    cursor.execute("SET NAMES 'UTF8MB4'")
    cursor.execute("SET CHARACTER SET UTF8MB4")


    pagina_atual, per_page, bearer_token = carregar_configuracoes(cursor, ini_file)
    url_base = 'https://stcorretora.digisac.me/api/v1/departments?page={}&perPage={}'  # Atualize para a URL correta
    headers = {
        'Authorization': f'Bearer {bearer_token}'
    }

    start_time = time.time()  # Iniciar o cronômetro total

    try:
        while True:
            url = url_base.format(pagina_atual, per_page)
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(f"Erro ao acessar API: {response.status_code}")

            data = response.json()
            items = data['data']
            pagination_info = {
                'total': data['total'],
                'limit': data['limit'],
                'skip': data['skip'],
                'currentPage': data['currentPage'],
                'lastPage': data['lastPage'],
                'from': data['from'],
                'to': data['to']
            }

            for item in items:
                inserir_dados_mysql(cursor, item, pagination_info)

            if pagina_atual >= pagination_info['lastPage']:
                break

            pagina_atual += 1

        conexao.commit()
        print(f"Dados inseridos com sucesso até a página {pagina_atual}")

    except Exception as e:
        conexao.rollback()
        print(f"Erro: {e}")
        traceback.print_exc()

    finally:
        cursor.close()
        conexao.close()

    total_time = time.time() - start_time  # Calcular o tempo total de execução
    print(f"Tempo total de execução: {total_time:.2f} segundos")

if __name__ == "__main__":
    processar_paginas()
