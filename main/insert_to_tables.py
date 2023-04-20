import psycopg2
from classes.engine import HH
from classes.dbmanager import DBManager
from datetime import datetime, date
from create_tables import create_all_objects
from pg_utils.utils import *


def insert_to_tables():
    # Выполняем проверку наличия таблиц в БД. Если хоть одной нет - создаем все таблицы заново и производим загрузку данных в них.
    # Если все есть - то сразу производим в них загрузку
    if DBManager().check_tables('currency', 'location', 'vacancy', 'employer') == False:
        create_all_objects()

    # Создаем список из 10 id понравившихся компаний
    company_list = [3388, 25880, 2733062, 4716984, 1122462, 64474, 4307, 4181, 2180, 733]

    # Наполняем список класса HH всеми возможными локациями работодателей
    HH.append_areas()
    # Подключаемся к БД
    connection = pg_connection()

    # Расклдаываем данные из списка в таблицу - LOCATION
    # Если вставляются дубли по ключу, то текущая строка обновляется по приницупу SCD1. Если Данных по ключу нет - данные вставляются
    for location_lvl_1 in HH.objects_list:
        pg_script(connection,
            f"""
            INSERT INTO LOCATION(processed_dttm, processed_dt, source, location_lvl, location_id, location_rk, location_name, parent_location_id, parent_location_rk)
            VALUES(  '{str(datetime.now())}'
                   , '{str(date.today())}'
                   , 'HH'
                   , 'LVL1'
                   , '{location_lvl_1['id']}'
                   , md5('HH' || '{location_lvl_1['id']}')
                   , '{location_lvl_1['name']}'
                   , '{location_lvl_1['parent_id']}'
                   , md5('HH' || '{location_lvl_1['parent_id']}')
            )
            ON CONFLICT(location_rk) DO UPDATE
            SET processed_dttm = '{str(datetime.now())}', processed_dt = '{str(date.today())}', location_name = '{location_lvl_1['name']}'
            """
        )
        for location_lvl_2 in location_lvl_1['areas']:
            pg_script(connection,
                f"""
                INSERT INTO LOCATION(processed_dttm, processed_dt, source, location_lvl, location_id, location_rk, location_name, parent_location_id, parent_location_rk)
                VALUES(  '{str(datetime.now())}'
                       , '{str(date.today())}'
                       , 'HH'
                       , 'LVL2'
                       , '{location_lvl_2['id']}'
                       , md5('HH' || '{location_lvl_2['id']}')
                       , '{location_lvl_2['name']}'
                       , '{location_lvl_2['parent_id']}'                   
                       , md5('HH' || '{location_lvl_2['parent_id']}')
                )
                ON CONFLICT(location_rk) DO UPDATE
                SET processed_dttm = '{str(datetime.now())}', processed_dt = '{str(date.today())}', location_name = '{location_lvl_2['name']}'
                """
            )
            for location_lvl_3 in location_lvl_2['areas']:
                pg_script(connection,
                    f"""
                    INSERT INTO LOCATION(processed_dttm, processed_dt, source, location_lvl, location_id, location_rk, location_name, parent_location_id, parent_location_rk)
                    VALUES(  '{str(datetime.now())}'
                           , '{str(date.today())}'
                           , 'HH'
                           , 'LVL3'
                           , '{location_lvl_3['id']}'
                           , md5('HH' || '{location_lvl_3['id']}')
                           , '{location_lvl_3['name']}'
                           , '{location_lvl_3['parent_id']}'  
                           , md5('HH' || '{location_lvl_3['parent_id']}')
                    )
                    ON CONFLICT(location_rk) DO UPDATE
                    SET processed_dttm = '{str(datetime.now())}', processed_dt = '{str(date.today())}', location_name = '{location_lvl_3['name']}'
                    """
                )
    #Закрываем соединение с БД
    pg_close_connection(connection)

    # Наполняем список класса HH всеми возможными валютами
    HH.append_dictionaries('currency')
    # Подключаемся к БД
    connection = pg_connection()

    # Расклдаываем данные из списка в таблицу валют - CURRENCY
    # Если вставляются дубли по ключу, то текущая строка обновляется по приницупу SCD1. Если Данных по ключу нет - данные вставляются
    for currency in HH.objects_list:
        pg_script(connection,
            f"""
            INSERT INTO CURRENCY(processed_dttm, processed_dt, source, currency_id, currency_rk, currency_name, currency_rub_rate)
            VALUES(  '{str(datetime.now())}'
                   , '{str(date.today())}'
                   , 'HH'
                   , '{currency['code']}'
                   , md5('HH' || '{currency['code']}')
                   , '{currency['name']}'
                   , 1 / {currency['rate']}
            )
            ON CONFLICT(currency_rk) DO UPDATE
            SET processed_dttm = '{str(datetime.now())}', processed_dt = '{str(date.today())}', currency_name = '{currency['name']}', currency_rub_rate = 1 / {currency['rate']}
            """
        )
    # Закрываем соединение с БД
    pg_close_connection(connection)

    # Наполняем список класса HH данными из списка работадателей, который определили в самом начале
    HH.append_employers(company_list=company_list)
    # Подключаемся к БД
    connection = pg_connection()

    # Расклдаываем данные из списка в таблицу работадателй - EMPLOYER
    # Если вставляются дубли по ключу, то текущая строка обновляется по приницупу SCD1. Если Данных по ключу нет - данные вставляются
    for employer in HH.objects_list:
        pg_script(connection,
                  f"""
                    INSERT INTO employer(processed_dttm, processed_dt, source, employer_id, employer_rk, employer_name, employer_url, location_rk)
                    VALUES(  '{str(datetime.now())}'
                           , '{str(date.today())}'
                           , 'HH'
                           , '{employer['id']}'
                           , md5('HH' || '{employer['id']}')
                           , '{employer['name']}'
                           , '{employer['site_url']}'
                           , md5('HH' || '{employer['area']['id']}')
                    )
                    ON CONFLICT(employer_rk) DO UPDATE 
                    SET processed_dttm = '{str(datetime.now())}', processed_dt = '{str(date.today())}',
                    employer_name = '{employer['name']}', employer_url = '{employer['site_url']}',
                    location_rk = md5('HH' || '{employer['area']['id']}');
                  """
                  )
    # Закрываем соединение с БД
    pg_close_connection(connection)

    # Вызываем функцию наполнения списка класса HH вакансиями 10 компаний, определенных в начале. Ищем по кл. словам: 'Developer, Analyst, SQL, Python'
    HH.append_vacancies(cicles_cnt=20, company_list=company_list, text='Developer, Analyst, SQL, Python')
    # Подключаемся к БД
    connection = pg_connection()

    # Раскладываем данные по вакансиям в таблицу VACANCY
    # Если вставляются дубли по ключу и данные отличаются - старая запись закрывается текущей DTTM, а новая открывается (valid_to_dttm = '9999-12-31') - SCD2
    for vacancy in HH.objects_list:
        #Реформатируем ЗП
        try:
            salary = vacancy['salary']
            if salary is None:
                salary['from'] = salary['to'] = salary['currency'] = salary['gross'] = 'None'

        except (KeyError, TypeError):
            salary = {}
            salary['from'] = salary['to'] = salary['currency'] = salary['gross'] = 'None'

        pg_script(connection,
            f"""
            INSERT INTO VACANCY(processed_dt, valid_from_dttm, valid_to_dttm, source, vacancy_id, vacancy_rk, vacancy_name,
            vacancy_url, vacancy_sal_from, vacancy_sal_to, employer_rk, currency_rk, location_rk, vacancy_sal_gross_flg)
            VALUES(  '{str(date.today())}'
                   , '{str(datetime.now())}'
                   , '{str('9999-12-31')}'
                   , 'HH'
                   , '{vacancy['id']}'
                   ,  md5('HH' || '{vacancy['id']}')
                   , '{vacancy['name']}'
                   , '{vacancy['alternate_url']}'
                   , CAST(NULLIF('{str(salary['from'])}', 'None') AS INT)
                   , CAST(NULLIF('{str(salary['to'])}', 'None') AS INT)
                   , md5('HH' || '{vacancy['employer']['id']}')
                   , md5('HH' || NULLIF('{salary['currency']}', 'None') )
                   , md5('HH' || '{vacancy['area']['id']}')
                   , CASE
                        WHEN '{salary['gross']}' = 'True'
                            THEN 1
                        WHEN '{salary['gross']}' = 'False'
                            THEN 0
                        ELSE NULL
                     END
            );
            """
        )
    # Так как в моей реализации таблица с вакансиями имеет историчность, апдейтим строки c предыдущей версией (если есть),
    # чтобы поддерживать основной принцип SCD2
    pg_script(connection,
        f"""
        UPDATE vacancy
        SET valid_to_dttm = current_timestamp
        WHERE (vacancy_rk, valid_from_dttm) IN (
                SELECT
                      vacancy_rk
                    , valid_from_dttm
                FROM (
                    SELECT
                          vacancy_rk
                        , valid_from_dttm
                        , valid_to_dttm
                        , ROW_NUMBER() OVER(PARTITION BY vacancy_rk ORDER BY valid_from_dttm DESC) AS RN
                    FROM vacancy
                )A
                WHERE A.RN = 2
                AND valid_to_dttm = '9999-12-31'
        );
        """
    )
    #Закрываем соединение с БД
    pg_close_connection(connection)

if __name__ == '__main__':
    insert_to_tables()


