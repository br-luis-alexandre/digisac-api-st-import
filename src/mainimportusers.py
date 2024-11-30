import requests
import mysql.connector
from configparser import ConfigParser
from datetime import datetime
import json
import traceback
import time

# Função para carregar as configurações do arquivo ini
def carregar_configuracoes(cursor, ini_file='config-users.ini'):
    config = ConfigParser()
    config.read(ini_file)
    
    pagina_inicial = int(config['API'].get('page', 0))
    per_page = int(config['API'].get('perPage', 3))  # Valor padrão é 3
    bearer_token = config['API'].get('bearerToken')

    sql = """
    DELETE FROM digisac_users WHERE current_page >= (%s);
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
    INSERT INTO digisac_users (
        id, name, email, phone_number, branch, 
        is_super_admin, is_client_user, created_at, 
        updated_at, deleted_at, account_id, archived_at, 
        sent_reset_password_at, is_first_login, timetable_id, 
        status, language, is_active_internal_chat, 
        internal_chat_token, otp_auth_active, 
        clients_status_app, clients_status_web,
        total, `limit`, skip, current_page, last_page, from_index, to_index
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Prepare data for insertion
    data = (
        item.get('id'),
        item.get('name'),
        item.get('email'),
        item.get('phoneNumber'),
        item.get('branch'),
        item.get('isSuperAdmin'),
        item.get('isClientUser'),
        format_datetime(item.get('createdAt')),
        format_datetime(item.get('updatedAt')),
        format_datetime(item.get('deletedAt')),
        item.get('accountId'),
        format_datetime(item.get('archivedAt')),
        format_datetime(item['data'].get('sentResetPasswordAt')),
        item.get('isFirstLogin'),
        item.get('timetableId'),
        item.get('status'),
        item.get('language'),
        item.get('isActiveInternalChat'),
        item.get('internalChatToken'),
        item.get('otpAuthActive'),
        item['clientsStatus'].get('app'),
        item['clientsStatus'].get('web'),
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
def processar_paginas(ini_file='config-users.ini'):
    inicio_total = time.time()
    
    conexao = criar_conexao_mysql()
    cursor = conexao.cursor()
    cursor.execute("SET NAMES 'UTF8MB4'")
    cursor.execute("SET CHARACTER SET UTF8MB4") 

    pagina_atual, per_page, bearer_token = carregar_configuracoes(cursor, ini_file)
    url_base = 'https://stcorretora.digisac.me/api/v1/users?page={}&perPage={}'  # Atualize para a URL correta
    headers = {
        'Authorization': f'Bearer {bearer_token}'
    }

    try:
        while True:
            inicio_processamento = time.time()  # Início do tempo para a página
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

            tempo_processamento = time.time() - inicio_processamento  # Tempo gasto para processar a página
            print(f"Tempo para processar a página {pagina_atual}: {tempo_processamento:.2f} segundos")

            if pagina_atual >= pagination_info['lastPage']:
                break

            pagina_atual += 1

        conexao.commit()
        tempo_total = time.time() - inicio_total  # Tempo total de execução
        print(f"Dados inseridos com sucesso até a página {pagina_atual}")
        print(f"Tempo total de execução: {tempo_total:.2f} segundos")

    except Exception as e:
        conexao.rollback()
        print(f"Erro: {e}")
        traceback.print_exc()

    finally:
        cursor.close()
        conexao.close()

if __name__ == "__main__":
    processar_paginas()
