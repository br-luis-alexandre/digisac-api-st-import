import requests
import mysql.connector
from configparser import ConfigParser
from datetime import datetime, timedelta
import json
import traceback
import time  # Importar a biblioteca time

# Função para carregar as configurações do arquivo ini
def carregar_configuracoes(cursor, ini_file='config-contacts.ini'):
    start_time = time.time()  # Iniciar o timer
    config = ConfigParser()
    config.read(ini_file)
    
    pagina_inicial = int(config['API'].get('page', 0))
    per_page = int(config['API'].get('perPage', 2))  # Valor padrão é 2
    bearer_token = config['API'].get('bearerToken')

    sql = """
    DELETE FROM digisac_contacts WHERE current_page >= (%s);
    """
    data = (str(pagina_inicial),)
    cursor.execute(sql, data)
    
    end_time = time.time()  # Finalizar o timer
    print(f"Tempo de carregamento das configurações: {end_time - start_time:.2f} segundos")

    return pagina_inicial, per_page, bearer_token

# Função para atualizar a última página lida no arquivo ini
def atualizar_pagina_final(page, ini_file='config-contacts.ini'):
    config = ConfigParser()
    config.read(ini_file)
    config['API']['page'] = str(page)
    with open(ini_file, 'w') as configfile:
        config.write(configfile)

# Função para criar conexão com o banco de dados MySQL
def criar_conexao_mysql():
    return mysql.connector.connect(
        host='129.148.44.247',  # Altere conforme necessário
        user='dbempresas',       # Altere conforme necessário
        password=']@Ym"iH4\\%s&ijXF',  # Altere conforme necessário
        database='dbempresas'  # Altere conforme necessário
    )

def format_datetime(date_str):
    if date_str:
        date_obj = datetime.strptime(date_str[:-1], '%Y-%m-%dT%H:%M:%S.%f')
        date_obj -= timedelta(hours=3)
        return date_obj.strftime('%Y-%m-%d %H:%M:%S')
    return None

def inserir_dados_mysql(cursor, item, total, limit, skip, current_page, last_page, from_, to_):
    sql = """
    INSERT INTO digisac_contacts (
        id, unsubscribed, is_me, is_group, is_broadcast, 
        unread, is_silenced, is_my_contact, had_chat, visible, 
        note, last_message_at, last_message_id, account_id, service_id, 
        person_id, default_department_id, default_user_id, created_at, updated_at, 
        deleted_at, current_ticket_id, status, last_contact_message_at, hsm_expiration_time, 
        block, data_block, archived_at, name, internal_name, 
        alternative_name, data_number, data_unread, data_last_sync_at, data_bot_is_running,
        data_bot_finished_at, last_message, total, `limit`, skip, 
        current_page, last_page, `from`, `to`
    ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
              %s, %s, %s, %s)
    """
    
    data_dict = item.get('data', {})
    data_number = data_dict.get('number', None)
    data_unread = data_dict.get('unread', None)
    data_last_sync_at = data_dict.get('lastSyncAt', None)
    data_bot_is_running = data_dict.get('botIsRunning', None)
    data_bot_finished_at = data_dict.get('botFinishedAt', None)
    data_block = json.dumps(item.get('dataBlock', {}))

    data = (
        item.get('id', None), 
        item.get('unsubscribed', None), 
        item.get('isMe', None), 
        item.get('isGroup', None), 
        item.get('isBroadcast', None), 
        item.get('unread', None), 
        item.get('isSilenced', None),
        item.get('isMyContact', None), 
        item.get('hadChat', None), 
        item.get('visible', None),  
        item.get('note', None), 
        format_datetime(item.get('lastMessageAt', None)),   
        item.get('lastMessageId', None), 
        item.get('accountId', None),
        item.get('serviceId', None),
        item.get('personId', None), 
        item.get('defaultDepartmentId', None), 
        item.get('defaultUserId', None), 
        format_datetime(item.get('createdAt', None)),       
        format_datetime(item.get('updatedAt', None)),       
        format_datetime(item.get('deletedAt', None)), 
        item.get('currentTicketId', None), 
        item.get('status', None), 
        format_datetime(item.get('lastContactMessageAt', None)),  
        item.get('hsmExpirationTime', None), 
        item.get('block', None), 
        data_block,                                   
        format_datetime(item.get('archivedAt', None)),      
        item.get('name', None),  
        item.get('internalName', None), 
        item.get('alternativeName', None), 
        data_number, 
        data_unread, 
        format_datetime(data_last_sync_at),           
        data_bot_is_running, 
        format_datetime(data_bot_finished_at),        
        item.get('lastMessage', None), 
        total, 
        limit, 
        skip, 
        current_page, 
        last_page, 
        from_, 
        to_
    )

    cursor.execute(sql, data)

# Função principal para processar as páginas
def processar_paginas(ini_file='config-contacts.ini'):
    start_time = time.time()  # Iniciar o timer total
    conexao = criar_conexao_mysql()
    cursor = conexao.cursor()
    cursor.execute("SET NAMES 'UTF8MB4'")
    cursor.execute("SET CHARACTER SET UTF8MB4") 

    pagina_atual, per_page, bearer_token = carregar_configuracoes(cursor, ini_file)
    url_base = 'https://stcorretora.digisac.me/api/v1/contacts?page={}&perPage={}'
    headers = {
        'Authorization': f'Bearer {bearer_token}'
    }

    try:
        while True:
            url = url_base.format(pagina_atual, per_page)
            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(f"Erro ao acessar API: {response.status_code}")

            data = response.json()
            items = data['data']
            total = data['total']
            limit = data['limit']
            skip = data['skip']
            current_page = data['currentPage']
            last_page = data['lastPage']
            from_ = data['from']
            to_ = data['to']

            # Inserir cada item no banco de dados
            for index, item in enumerate(items):
                insert_start_time = time.time()  # Iniciar o timer da inserção
                print(f"Inserindo item {index + 1} da página {pagina_atual}...")
                inserir_dados_mysql(cursor, item, total, limit, skip, current_page, last_page, from_, to_)
                insert_end_time = time.time()  # Finalizar o timer da inserção
                #print(f"Tempo de inserção do item {index + 1}: {insert_end_time - insert_start_time:.2f} segundos")

            # Se chegarmos à última página, sair do loop
            if pagina_atual >= last_page:
                break

            # Próxima página
            pagina_atual += 1

            conexao.commit()
            atualizar_pagina_final(pagina_atual, ini_file)

        conexao.commit()
        print(f"Dados inseridos com sucesso. Última página lida: {pagina_atual}")
        atualizar_pagina_final(pagina_atual, ini_file)

    except Exception as e:
        conexao.rollback()
        print(f"Erro: {e}")
        print("Detalhes do erro:")
        traceback.print_exc()

    finally:
        cursor.close()
        conexao.close()
        end_time = time.time()  # Finalizar o timer total
        print(f"Tempo total de processamento: {end_time - start_time:.2f} segundos")

if __name__ == "__main__":
    processar_paginas()
