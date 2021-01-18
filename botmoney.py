import vk_api
import random
from psycopg2 import sql
import psycopg2
from datetime import datetime
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
import json
import re
import calendar
import requests
import os

token = "***"
vk = vk_api.VkApi(token=token)
# longpoll = VkLongPoll(vk)
longpoll = VkBotLongPoll(vk, '***')

 

        # if event.type == VkBotEventType.MESSAGE_NEW:


user_id='id'
user_id_int=id

connection_parameters = {
    'dbname': 'Base',
    'user': 'admin',
    'password': 'pass',
    'port':'port',
    'host': 'ip'
    }


def update_balance_analiz(type,balance):
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        values = [(type,balance,datetime.today())]
        conn.autocommit = True
        insert = sql.SQL('INSERT INTO money_analiz (type,balance,date) VALUES {}').format(sql.SQL(',').join(map(sql.Literal, values)))
        cursor.execute(insert)

# Удаление последнего
def delete_last():
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        conn.autocommit = True
        delete = sql.SQL('DELETE FROM money_analiz WHERE code = (SELECT code FROM money_analiz ORDER BY code DESC LIMIT 1)')
        cursor.execute(delete)
    conn.close()

# Отправка вк
def write_msg(message):
    vk.method('messages.send', {'user_id': user_id, 'random_id': random.randint(1,999999999),'message': message,'keyboard':json.dumps({'buttons':[]})})

# Выборка Всего баланса
def select_from_acc(message):
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        cursor.execute("select balance,name from money_account ORDER BY code")
        for row in cursor:
            message+= row[1]+' '+row[0]+'\n'
    conn.close()
    return message

# Обновление баланса счета
def update_money_acc(balance,type):
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        conn.autocommit = True
        update = sql.SQL("update money_account set balance ='"+str(balance)+"' where name = '"+type+"'")
        cursor.execute(update)
    conn.close()

# Выборка баланса
def select_balance(type):
    balance = 0
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        cursor.execute("select balance from money_account where name = '"+type+"'")
        for row in cursor:
            balance = int(row[0])
    conn.close()
    return balance

# Выборка имени счета
def select_name_from_acc(synonym):
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        cursor.execute("select name from money_account where POSITION ('"+synonym+"' in synonyms) <> '0'")
        if(cursor.rowcount == 0):
            write_msg('Неправильно указан счет')
            type='false'
        for row in cursor:
            type = row[0]
    conn.close()
    return type

# Проверка есть ли комент
def check_comment(commentar):
    comment=''
    if len(commentar)>1:
        comment=commentar[1]
    return comment

# Помощь
def help():
    conn = psycopg2.connect(**connection_parameters)
    with conn.cursor() as cursor:
        message='Напоминалка\nСчета:\n'
        cursor.execute("select name,synonyms from money_account order by name")
        for row in cursor:
            message+= row[0]+' '+row[1]+'\n'
    with conn.cursor() as cursor:
        message+='Расходы:\n'
        cursor.execute("select name,synonyms,view from money_category_types where view='Расходы' order by name")
        for row in cursor:
            message+= row[0]+' '+row[1]+'\n'
    with conn.cursor() as cursor:
        message+='Доходы:\n'
        cursor.execute("select name,synonyms,view from money_category_types where view='Доходы' order by name")
        for row in cursor:
            message+= row[0]+' '+row[1]+'\n'
    conn.close()
    message+='Примеры:\nСбер 100 еда или Перевод сбер вклад 1000'
    return message

def select_delta_date(type,firstdate):
    conn = psycopg2.connect(**connection_parameters)
    delta = 0
    with conn.cursor() as cursor:
        cursor.execute("select SUM(cast(sum as int)) from money_analiz where type = '"+type+"' and date = '"+firstdate.strftime('%Y-%m-%d')+"'")
        for row in cursor:
            delta = row[0]
    conn.close()
    return delta




try:
    for event in longpoll.listen():
        if event.type == VkBotEventType.MESSAGE_NEW:
            if event.obj.from_id == user_id_int:
                request = event.obj.text
                commentar = request.split("~")
                txt = commentar[0].split(" ")
                # расход доход
                # Перевод
                if txt[0]=='Перевод':
                    # Выборка типа счета
                    type = select_name_from_acc(txt[1])
                    if (type == 'false'):
                        continue
                    where = select_name_from_acc(txt[2])
                    if (where == 'false'):
                        continue
                    spent = txt[3]
                    typeoper=txt[0]
                    # Проверка комента
                    comment = check_comment(commentar)
                        
                   # Запись в базу
                    conn = psycopg2.connect(**connection_parameters)
                    with conn.cursor() as cursor:
                        values = [('перевод между счетами',typeoper,spent,type,where,datetime.today(),comment)]
                        conn.autocommit = True
                        insert = sql.SQL('INSERT INTO money_analiz (category,type,sum,type_money,dest,date,comment) VALUES {}').format(sql.SQL(',').join(map(sql.Literal, values)))
                        cursor.execute(insert)
                    
                    # минусуем откуда перевод
                    balance = select_balance(type)
                    balance -= int(spent);
                    update_money_acc(balance,type)
                    # Плюсуем куда переводим
                    balance = select_balance(where)
                    balance += int(spent);
                    update_money_acc(balance,where)
                    
                    # Проверка есть ли примечание и отправляем
                    message ='Записал ' + typeoper.lower()+' '+type.lower()+'->'+where.lower()+' сумму '+spent+'р.'
                    if comment!='':
                        message +='Примечание: '+comment+'\n'
                    else:
                        message +='\n'
                    write_msg(select_from_acc(message))
                # Баланс
                elif txt[0] == 'Баланс':
                    message='Сегодня: \n'
                    now = datetime.now()
                    message+='Расходы: '+str(select_delta_date('Расходы',now))+'\n'
                    message+='Доходы: '+str(select_delta_date('Доходы',now))+'\n'
                    conn = psycopg2.connect(**connection_parameters)
                    mes=''
                    with conn.cursor() as cursor:
                        cursor.execute("select type,sum,type_money,category,comment from money_analiz where date = '"+now.strftime('%Y-%m-%d')+"' and not type is null")
                        for row in cursor:
                            message+=row[0]+' '+row[2]+' '+row[3]+' '+row[1]
                            if row[4] is not None:
                                message+=' '+row[4]
                            message+='\n'
                        conn.close()
                    conn = psycopg2.connect(**connection_parameters)
                    
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT sum(cast(sum as int)),category FROM money_analiz where date = '"+now.strftime('%Y-%m-%d')+"' and category in (select name from money_category_types where view = 'Расходы' and viewonreport is null) GROUP BY category order by sum(cast(sum as int)) ")
                        for row in cursor:
                            mes='Максимальные траты '+str(row[0])+' '+row[1]+'\n'
                        conn.close()
                    message+=mes
                    write_msg(select_from_acc(message))
                # Последние выборка
                elif re.fullmatch('[0-9]*', txt[0]):
                    message='От старого к новому \n'
                    conn = psycopg2.connect(**connection_parameters)
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT type,sum,type_money,dest,date FROM money_analiz where not type is null ORDER BY code DESC LIMIT "+txt[0])
                        for row in cursor:
                            message += row[0]+' '+row[2].lower()+' '
                            if row[3] is not None:
                                message += row[3].lower()
                            message += ' от '+str(row[4])+' на '+str(row[1])+'р.\n'
                    conn.close()
                    write_msg(message)
                # help
                elif txt[0] == 'Забыл':
                    write_msg(help())
                # Удаление
                elif txt[0] == 'Удали':
                    # Выбор последней операции
                    conn = psycopg2.connect(**connection_parameters)
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT type,sum,type_money,dest,date FROM money_analiz ORDER BY code DESC LIMIT 1")
                        for row in cursor:
                            type = row[0]
                            sum = int(row[1])
                            type_money = row[2]
                            dest = row[3]
                            date = row[4]
                    conn.close()
                    # В зависимости от операции чиним баланс
                    if type == 'Доходы':
                        balance = select_balance(type_money)
                        balance-=sum
                        update_money_acc(balance,type_money)
                        message='Удалил '+type.lower()+' с '+type_money.lower()+' на сумму '+str(sum)+'р. от '+str(date)+'\n'
                    elif type == 'Расходы':
                        balance = select_balance(type_money)
                        balance+=sum
                        update_money_acc(balance,type_money)
                        message='Удалил '+type.lower()+' с '+type_money.lower()+' на сумму '+str(sum)+'р. от '+str(date)+'\n'
                    elif type == 'Перевод':
                        balance = select_balance(type_money)
                        balance+=sum
                        update_money_acc(balance,type_money)
                        balance = select_balance(dest)
                        balance-=sum
                        update_money_acc(balance,dest)
                        message='Удалил '+type.lower()+' '+type_money.lower()+'->'+dest.lower()+' на сумму '+str(sum)+'р. от '+str(date)+'\n'
                    # Удаляем последнюю операцию
                    delete_last()
                    write_msg(select_from_acc(message))
                else:
                    if len(txt)==3:
                        # Выбор счета
                        type = select_name_from_acc(txt[0])
                        if (type == 'false'):
                            continue
                        # Выбор категории
                        conn = psycopg2.connect(**connection_parameters)
                        with conn.cursor() as cursor:
                            cursor.execute("select name,view from money_category_types where POSITION ('"+txt[2]+"' in synonyms) <> '0'")
                            if(cursor.rowcount == 0):
                                write_msg('Неправильно указана категория')
                                continue
                            for row in cursor:
                                category = row[0]
                                typeoper = row[1]
                        conn.close()
                        
                        # Сумма
                        spent = txt[1]
                        
                        # Проверка есть ли комент
                        comment = check_comment(commentar)
                        
                        # Запись в базу
                        conn = psycopg2.connect(**connection_parameters)
                        with conn.cursor() as cursor:
                            values = [(typeoper,spent,type,datetime.today(),category,comment)]
                            conn.autocommit = True
                            insert = sql.SQL('INSERT INTO money_analiz (type,sum,type_money,date,category,comment) VALUES {}').format(sql.SQL(',').join(map(sql.Literal, values)))
                            cursor.execute(insert)
                        conn.close()
                        
                        # Баланс
                        balance = select_balance(type)
                        
                        # В зависимости от операции сумма или разность
                        if typeoper == 'Доходы':
                            balance += int(spent);
                        if typeoper == 'Расходы':
                            balance -= int(spent);
                        
                        # Обновление счета
                        update_money_acc(balance,type)
                        
                        # Отправка сообщения
                        message ='Записал в ' + typeoper.lower()+'->'+category.lower()+'. Сумма: '+spent+'р. с '+type.lower()
                        if comment!='':
                            message +='Примечание: '+comment+'\n'
                        else:
                            message +='\n'
                        write_msg(select_from_acc(message))
                    else:
                        write_msg('Допустил ошибку')
except (requests.exceptions.ConnectionError, TimeoutError, requests.exceptions.Timeout,requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout):
    os.startfile('vkbot.cmd')