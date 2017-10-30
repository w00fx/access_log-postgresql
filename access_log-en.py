import os
from pg import DB
import sys
import time


def conectar_db():
    try:
        banco_nome = input('Enter the name of the database that will be used: ')
        host = input('Enter the host ip, if it is local enter "localhost": ')
        porta = int(input('Enter the port "default port is 5432": '))
        usuario = input('Enter the user: ')
        senha = input('Enter the password: ')
        db = DB(dbname=banco_nome, host=host,
                port=porta, user=usuario, passwd=senha)
        print('Connected!')
        input('Press any key to continue ...')
        os.system('cls')
        return db

    except KeyboardInterrupt:
        print('Parando...')
        sys.exit()

    except Exception as e:
        os.system('cls')
        print('The described error was: ', e)
        print('Let\'s try again...')
        input('Press any key to continue ...')
        conectar_db()


def criar_tabelas(banco):
    try:
        print('Creating tables')
        banco.query("""CREATE TABLE public.access_log (
                    id serial,
                    ip VARCHAR(70) NOT NULL,
                    identify_check VARCHAR(50) NOT NULL,
                    userid VARCHAR(30) NOT NULL,
                    horario TIMESTAMP NOT NULL,
                    zona_h VARCHAR NOT NULL,
                    request VARCHAR NOT NULL,
                    status_code VARCHAR(5) NOT NULL,
                    size_object VARCHAR NOT NULL,
                    CONSTRAINT access_log_pk PRIMARY KEY (id)
    )""")
        print('Tables created!')
    except Exception:
        print('Make sure the tables are already created. If not, check your database.')


def inserir_dados(db, ip, id_check, user_id, horar, zona_h, com, status_code, size_object):
    db.insert('access_log', ip=ip, identify_check=id_check,
              userid=user_id, horario=horar, zona_h=zona_h, request=com,
              status_code=status_code, size_object=size_object)


def arq_controle(arq):
    if not os.path.exists(arq):
        if 'control_accesslog' == arq:
            file = open(arq, 'w')
            file.write('Last accessed logs\n\n')
        elif 'logs_error' == arq:
            file = open(arq, 'w')
            file.write('Latest logs with error\n\n')

        try:
            global cont
            cont = file.read()
        except IOError:
            arq_controle(arq)
        file.close()
        return cont
    else:
        file = open(arq)
        cont = file.read()
        file.close()
        return cont


def esc_controle(f_log, arq):
    file = open(arq, 'a')
    file.write(f_log + '\n')
    file.close()


def acessarAccessLog(banco, logs):
    with open(logs) as access:
        arq = arq_controle('control_accesslog')
        access = reversed(list(access))
        j = 0
        for i in access:
            if i not in arq:
                try:
                    i = i.split()
                    ip = i[0]
                    id_check = i[1]
                    user_id = i[2]
                    horar = i[3]
                    zona_h = i[4]
                    com = i[5] + ' ' + i[6] + ' ' + i[7]
                    status_code = i[8]
                    size_object = i[9]
                    inserir_dados(banco, ip, id_check, user_id,
                                  horar, zona_h[0:5], com, status_code, size_object)
                except IndexError:
                    esc_controle(' '.join(i), 'logs_error')
                    print('Some logs have errors to insert.')
                    print('Check in logs_error.')
            else:
                return i
            if j == 0:
                esc_controle(' '.join(i), 'control_accesslog')
                j = 1


try:
    print('Creating program\'s control structure...')
    arq_controle('control_accesslog')
    arq_controle('logs_error')

    print('Make sure you have a database created to receive the logs!')
    print('Before let\'s connect in the database...\n\n')
    banco = conectar_db()
    logs = input('Enter the path of access_log: ')
    while True:
        print('1 - Create tables')
        print('2 - Search for data and insert in database')
        print('3 - Leave him searching for logs every 30s')
        print('0 - Quit')
        opcao = input('\nEnter your option:')

        if opcao == '1':
            criar_tabelas(banco)
            input('Press any key to continue ...')
            os.system('cls')

        elif opcao == '2':
            acessarAccessLog(banco, logs)
            print('Logs inserted!')
            input('Press any key to continue ...')
            os.system('cls')

        elif opcao == '3':
            try:
                seg = int(input(
                    'Enter the seconds for the program to search logs: '))
            except ValueError:
                print('Enter in numbers. Let\'s try again.\n\n\n')

            print('To stop use CTRL+C')
            while True:
                try:
                    acessarAccessLog(banco, logs)
                    time.sleep(seg)
                except KeyboardInterrupt:
                    print('Stopping....\n')
                    input('Press any key to continue ...')
                    os.system('cls')
                    break

        elif opcao == '0':
            sys.exit('Closing program.')

        else:
            print('Enter one of the options.')

except KeyboardInterrupt:
    print('Closing program.')
