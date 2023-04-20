import psycopg2
from pg_utils.pg_config import db_name, user, password, host


class DBManager:
    __slots__ = ('dbname', 'user', 'password', 'host', 'port', 'conn', 'cur')

    def __init__(self, dbname=db_name, user=user, password=password, host=host, port='5432'):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        """Устанавливает соединение с БД"""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
        except psycopg2.Error as e:
            print(f"Ошибка подключения к БД: {e}")

    def disconnect(self):
        """Закрывает соединение с БД"""
        if self.conn:
            self.cur.close()
            self.conn.close()

    def adhoc_query(self, query):
        """Свободный запрос к БД"""
        self.connect()
        self.cur.execute(query)
        self.disconnect()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        self.connect()
        self.cur.execute(
            """
                SELECT 
                      e.employer_name 
                    , count(*) as vacancy_cnt
                FROM vacancy v 
                LEFT JOIN employer e ON v.employer_rk = e.employer_rk
                WHERE v.valid_to_dttm = '9999-12-31'
                GROUP BY e.employer_name
                ORDER BY count(*) DESC;
            """
        )
        companies = self.cur.fetchall()
        self.disconnect()
        return companies

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия ваканси, зарплаты и ссылки на вакансию"""
        self.connect()
        self.cur.execute(
            """
                SELECT 
                      e.employer_name 
                    , v.vacancy_name
                    , v.vacancy_sal_from
                    , v.vacancy_sal_to
                    , c.currency_name
                    , v.vacancy_url
                FROM vacancy v 
                LEFT JOIN employer e ON v.employer_rk = e.employer_rk
                LEFT JOIN currency c ON c.currency_rk = v.currency_rk
                WHERE v.valid_to_dttm = '9999-12-31'
                ORDER BY vacancy_sal_to, vacancy_sal_from, employer_name;
            """
        )
        vacancies = self.cur.fetchall()
        self.disconnect()
        return vacancies

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям (в рублях)"""
        self.connect()
        self.cur.execute(
            """
                SELECT 
                    round(avg(CASE 
                                WHEN v.vacancy_sal_gross_flg = 0 THEN (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate
                                ELSE (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate * 0.87
                              END), 0) AS avg_sal
                FROM vacancy v
                LEFT JOIN currency c ON c.currency_rk = v.currency_rk
                WHERE v.valid_to_dttm = '9999-12-31'
                AND (vacancy_sal_from IS NOT NULL OR vacancy_sal_to IS NOT NULL);
            """
        )
        avg_salary = self.cur.fetchone()[0]
        self.disconnect()
        return avg_salary

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней """
        self.connect()
        self.cur.execute(
            f"""
                SELECT 
                      vacancy_name
                    , vacancy_url
                    , CAST(avg_sal AS INT) AS avg_rub_sal_net
                FROM (
                        SELECT 
                              v.vacancy_name
                            , v.vacancy_url
                            , v.vacancy_sal_from
                            , v.vacancy_sal_to
                            , c.currency_rub_rate
                            , round(
                                    CASE 
                                        WHEN v.vacancy_sal_gross_flg = 0 THEN (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate
                                        ELSE  (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate * 0.87
                                    END, 0) AS avg_sal
                            , round(avg(
                                    CASE 
                                        WHEN v.vacancy_sal_gross_flg = 0 THEN (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate					
                                        ELSE (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate * 0.87
                                    END 
                            ) OVER(), 0)AS avg_sal_all
                                
                        FROM vacancy v
                        LEFT JOIN currency c ON c.currency_rk = v.currency_rk
                        WHERE v.valid_to_dttm = '9999-12-31'
                        AND (vacancy_sal_from IS NOT NULL OR vacancy_sal_to IS NOT NULL)
                )a
                WHERE avg_sal > avg_sal_all;
            """
        )
        vacancies = self.cur.fetchall()
        self.disconnect()
        return vacancies

    def get_vacancies_with_keyword(self, keyword):
        """Получает список всех вакансий, в названиях которых содержится переданное в метод ключевое слово"""
        self.connect()
        self.cur.execute(
            f"""
                SELECT 
                      v.vacancy_name
                    , v.vacancy_url
                FROM vacancy v 
                WHERE upper(vacancy_name) LIKE upper('%{keyword}%')
                AND v.valid_to_dttm = '9999-12-31';
            """
        )
        vacancies = self.cur.fetchall()
        self.disconnect()
        return vacancies

    def check_tables(self, *tables):
        """
        Выполняет проверку наличия в БД, указанных в параметрах таблиц.
        Если все таблицы есть - возвращает True, если хоть одной нет - False"""
        self.connect()
        self.cur.execute(
            f"""
                SELECT DISTINCT
                    lower(TABLE_NAME)
                FROM information_schema.tables
                WHERE TABLE_NAME IN {tables}
            """
        )
        vacancies = self.cur.fetchall()
        self.disconnect()
        return len(vacancies) == len(tables)