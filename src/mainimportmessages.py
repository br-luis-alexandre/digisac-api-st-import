import requests
import mysql.connector
from configparser import ConfigParser
from datetime import datetime, timedelta
import time

# Função para carregar as configurações do arquivo ini
def carregar_configuracoes(cursor, ini_file='config-messages.ini'):
    config = ConfigParser()
    config.read(ini_file)
    
    pagina_inicial = int(config['API'].get('page', 1))
    per_page = int(config['API'].get('perPage', 500))
    bearer_token = config['API'].get('bearerToken')

    sql = """
    DELETE FROM digisac_messages WHERE current_page >= (%s);
    """
    data = (str(pagina_inicial),)
    cursor.execute(sql, data)
    
    return pagina_inicial, per_page, bearer_token

# Função para atualizar a última página lida no arquivo ini
def atualizar_pagina_final(page, ini_file='config-messages.ini'):
    config = ConfigParser()
    config.read(ini_file)
    config['API']['page'] = str(page)
    with open(ini_file, 'w') as configfile:
        config.write(configfile)

# Função para criar conexão com o banco de dados MySQL
def criar_conexao_mysql():
    return mysql.connector.connect(
        host='129.148.44.247',
        user='dbempresas',
        password=']@Ym"iH4\\%s&ijXF',
        database='dbempresas'
    )

def format_datetime(date_str):
    if date_str:
        date_obj = datetime.strptime(date_str[:-1], '%Y-%m-%dT%H:%M:%S.%f')
        date_obj -= timedelta(hours=3)
        return date_obj.strftime('%Y-%m-%d %H:%M:%S')
    return None

# Função para inserir os dados no banco de dados
def inserir_dados_mysql(cursor, item, total, limit, skip, current_page, last_page, from_, to_):
    sql = """
    INSERT INTO digisac_messages (
        id, is_from_me, sent, type, timestamp, timestamp_text, data_ticket_open, data_ticket_transfer, data_ticket_close, data_ack, data_is_new, data_is_first, data_file_ended_at,
        data_file_ended_at_text, data_file_started_at, data_file_started_at_text, data_file_is_downloading, visible, account_id, contact_id, from_id, service_id, 
        to_id, user_id, ticket_id, ticket_user_id, ticket_department_id, quoted_message_id, origin, created_at, updated_at, created_at_text, updated_at_text, 
        deleted_at, hsm_id, is_comment, reaction_parent_message_id, is_transcribing, transcribe_error, text, obfuscated, 
        files, publicFilename, quoted_message, is_from_bot, actiontransfer, comments, toDepartmentId, fromDepartmentId, toUserId, fromUserId, total, `limit`, skip, current_page, last_page, `from`, `to`
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s, %s,%s, %s, %s,%s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    

    file_download = item['data'].get('fileDownload', {})
    ended_at_text = file_download.get('endedAt', None)
    started_at_text = file_download.get('startedAt', None)
    ended_at = format_datetime(file_download.get('endedAt', None))
    started_at = format_datetime(file_download.get('startedAt', None))
    is_downloading = file_download.get('isDownloading', False)

    if(item['file']):
        file = item['file'].get('url', None)
        publicFilename = item['file'].get('publicFilename', None)
        response = requests.get(file)
        with open('downloads/'+publicFilename, "wb") as arquivo:
          arquivo.write(response.content)

    else:
        file = None
        publicFilename = None


    #print(item.get('id'))
    actiontransfer = ''
    comments = ''
    toDepartmentId = ''
    fromDepartmentId = ''
    toUserId = ''
    fromUserId = ''
    if(item['ticket']):
        tichet = item['ticket']
        for i in tichet['ticketTransfers']:
            if(item.get('id') == i['transferredMessageId']):
                #print(i['comments'])
                #print(i['action'])
                #print(i['transferredMessageId'])
                actiontransfer = i['action']
                comments = i['comments']
                toDepartmentId = i['toDepartmentId']
                fromDepartmentId = i['fromDepartmentId']
                toUserId = i['toUserId']
                fromUserId = i['fromUserId']

            
  
   

    data = (
        item.get('id'), 
        item.get('isFromMe', False), 
        item.get('sent', False), 
        item.get('type'), 
        format_datetime(item.get('timestamp', None)), 
        item.get('timestamp', None), 
        item['data'].get('ticketOpen', False),
        item['data'].get('ticketTransfer', False),
        item['data'].get('ticketClose', False),
        item['data'].get('ack', None), 
        item['data'].get('isNew', False), 
        item['data'].get('isFirst', False), 
        ended_at,
        ended_at_text,
        started_at,
        started_at_text,
        is_downloading,
        item.get('visible', True), 
        item.get('accountId'), 
        item.get('contactId'), 
        item.get('fromId'), 
        item.get('serviceId'), 
        item.get('toId', None), 
        item.get('userId', None), 
        item.get('ticketId'), 
        item.get('ticketUserId', None), 
        item.get('ticketDepartmentId'), 
        item.get('quotedMessageId', None), 
        item.get('origin', None), 
        format_datetime(item.get('createdAt', None)), 
        format_datetime(item.get('updatedAt', None)), 
        item.get('createdAt', None), 
        item.get('updatedAt', None), 
        item.get('deletedAt', None), 
        item.get('hsmId', None), 
        item.get('isComment', False), 
        item.get('reactionParentMessageId', None), 
        item.get('isTranscribing', None), 
        item.get('transcribeError', None), 
        item.get('text', ''), 
        item.get('obfuscated', False), 
        file,  #item.get('file.url', None), 
        publicFilename,
        item.get('quotedMessage', None), 
        item.get('isFromBot', False), 
        actiontransfer,
        comments,
        toDepartmentId,
        fromDepartmentId,
        toUserId,
        fromUserId, 
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
def processar_paginas(ini_file='config-messages.ini'):
    start_time = time.time()  # Timer total
    conexao = criar_conexao_mysql()
    cursor = conexao.cursor()
    cursor.execute("SET NAMES 'UTF8MB4'")
    cursor.execute("SET CHARACTER SET UTF8MB4") 

    pagina_atual, per_page, bearer_token = carregar_configuracoes(cursor, ini_file)
    url = 'https://stcorretora.digisac.me/api/v1/messages'
    url_base = url + '?page={}&perPage={}&query={}'
    query = '{"where":{"contactId":"a9ce322f-81e2-4d50-88eb-fd214a9158fc"},"include":["file","files",{"model":"ticket","include":["ticketTransfers"]},{"model":"user","attributes":["id","name"]},{"model":"ticketTransfer","include":["fromUser","toUser","fromDepartment","toDepartment","byUser"]},{"model":"from","attributes":["id","name","alternativeName","internalName"]},{"model":"contact","attributes":["id"]}]}'

    #url = 'https://stcorretora.digisac.me/api/v1/messages'
    #url_base = url + '?page={}&perPage={}'

    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'X-Return-Base64': 'true'
    }

    try:
        while True:
            page_start_time = time.time()  # Timer para a página atual
            url = url_base.format(pagina_atual, per_page, query)
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

            print(f"Processando página {pagina_atual} de {last_page} (itens {from_} a {to_})")

            for index, item in enumerate(items, start=1):
                inserir_dados_mysql(cursor, item, total, limit, skip, current_page, last_page, from_, to_)
                print(f"  - Item {index} de {len(items)} inserido na página {pagina_atual}")

            page_duration = time.time() - page_start_time  # Duração da página atual
            print(f"Tempo para processar a página {pagina_atual}: {page_duration:.2f} segundos")

            if pagina_atual >= last_page:
                break

            pagina_atual += 1
            
            conexao.commit()
            atualizar_pagina_final(pagina_atual, ini_file)

        conexao.commit()
        atualizar_pagina_final(pagina_atual, ini_file)

    except Exception as e:
        conexao.rollback()
        print(f"Erro: {e}")
        print(f"Erro Completo: {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    finally:
        cursor.close()
        conexao.close()
        total_duration = time.time() - start_time  # Duração total
        print(f"Tempo total de processamento: {total_duration:.2f} segundos")

if __name__ == "__main__":
    processar_paginas()