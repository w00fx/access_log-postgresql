import os
from pg import DB
import sys
import time


def conectar_db():
    try:
        banco_nome = input('Insira o nome do banco que sera usado: ')
        host = input('Insira o ip do host, se for local insira "localhost": ')
        porta = int(input('Insira a porta "porta padrao e 5432": '))
        usuario = input('Insira o usuario: ')
        senha = input('Insira a senha: ')
        db = DB(dbname=banco_nome, host=host,
                port=porta, user=usuario, passwd=senha)
        print('Conectado!')
        input('Pressione qualquer tecla para continuar...')
        os.system('cls')
        return db

    except KeyboardInterrupt:
        print('Parando...')
        sys.exit()

    except Exception as e:
        os.system('cls')
        print('O erro descrito foi: ', e)
        print('Vamos tentar novamente...')
        input('Pressione qualquer tecla para continuar...')
        conectar_db()


def criar_tabelas(banco):
    try:
        print('Criando tabelas...')
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
        print('Tabelas criadas!')
    except Exception:
        print('Verifique se as tabelas ja estao criadas.')


def inserir_dados(db, ip, id_check, user_id, horar, zona_h, com, status_code, size_object):
    db.insert('access_log', ip=ip, identify_check=id_check,
              userid=user_id, horario=horar, zona_h=zona_h, request=com,
              status_code=status_code, size_object=size_object)


def arq_controle(arq):
    if not os.path.exists(arq):
        if 'controle_accesslog' == arq:
            file = open(arq, 'w')
            file.write('Ultimos logs acessados\n\n')
        elif 'logs_erro' == arq:
            file = open(arq, 'w')
            file.write('Ultimos logs com erro\n\n')

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


def acessarAccessLog(banco):
    with open('access_log') as access:
        arq = arq_controle('controle_accesslog')
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
                    esc_controle(' '.join(i), 'logs_erro')
                    print('Alguns logs apresentaram erros para serem inseridos.')
                    print('Verifique no logs_erro.')
            else:
                return i
            if j == 0:
                esc_controle(' '.join(i), 'controle_accesslog')
                j = 1


try:
    print('Criando estrutura de controle...')
    arq_controle('controle_accesslog')
    arq_controle('logs_erro')

    print('Certifique que tenha um banco de dados criado para receber os logs!')
    print('Antes vamos no conectar no banco de dados...\n\n')
    banco = conectar_db()
    while True:
        print('1 - Criar tabelas')
        print('2 - Buscar por dados e inserir no banco')
        print('3 - Deixar ele buscando por dados a cada 30s')
        print('0 - Sair')
        opcao = input('\nInsira sua opcao:')

        if opcao == '1':
            criar_tabelas(banco)
            input('Pressione qualquer tecla para continuar...')
            os.system('cls')

        elif opcao == '2':
            acessarAccessLog(banco)
            print('Dados inseridos!')
            input('Pressione qualquer tecla para continuar...')
            os.system('cls')

        elif opcao == '3':
            try:
                seg = int(input(
                    'Insira de quanto em quanto tempo que voce quer ele quer que verifique os logs: '))
            except ValueError:
                print('Insira em numeros. Vamos tentar novamente.\n\n\n')

            print('Para parar use CTRL+C')
            while True:
                try:
                    acessarAccessLog(banco)
                    time.sleep(seg)
                except KeyboardInterrupt:
                    print('Parando a verificacao por tempo.\n')
                    input('Pressione qualquer tecla para continuar...')
                    os.system('cls')
                    break

        elif opcao == '0':
            sys.exit('Encerrando programa...')

        else:
            print('Insira uma opcao correta.')

except KeyboardInterrupt:
    print('Fim de programa.')
