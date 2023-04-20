import psycopg2
from pg_utils.pg_config import host, user, password, db_name


def pg_connection(host=host, user=user, password=password, database=db_name, autocommit = True):
    """
    Подключается к бд по преквизитам из файла:
    pg_config.py (по дефолту), или по заданным вручную
    """
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name
    )
    connection.autocommit = autocommit  # Выставляем автоматический commit
    return connection

def pg_close_connection(connection):
    """Закрывает сессию в БД PostgresSQL"""
    connection.close()

def pg_script(connection, query='select 1'):
    """
    Выполняет запрос к БД:
    connection - реквизиты подключения
    query - запрос к БД
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""{query}"""
            )
            return cursor.fetchall()
    # Обрабатываем ошибки
    except psycopg2.ProgrammingError:  # так как данная функция возвращает значения, при инсерте делите и апдейте - значения не возвращаются. Следовательно - ошибка
        pass
    except Exception as _ex:
        print('[INFO] Error while working with PostgreSQL', _ex)

def pg_insert(connection, table_name, **columns):
    """
    Записывает данные в таблицу
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                    f"""
                    INSERT INTO {table_name}({",".join(columns.keys())})
                    VALUES('{"','".join(columns.values())}')
                    """
            )
            return cursor.fetchall()
            # Обрабатываем ошибки
    except psycopg2.ProgrammingError:  # так как данная функция возвращает значения, при инсерте, делите и апдейте - значения не возвращаются. Следовательно - ошибка
        pass
    except Exception as _ex:
        print('[INFO] Error while working with PostgreSQL', _ex)

