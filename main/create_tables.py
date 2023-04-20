from classes.dbmanager import DBManager


def create_all_objects():
    """
    Создает таблицы: location, currency, employer, vacancy;
    Функцию prevent_duplicate_insert, и триггер к ней - check_duplicates
    """
    # Создаем таблицу содержащую информацию о локации - location
    DBManager().adhoc_query(
              f"""
                DROP TABLE IF EXISTS location CASCADE;
                CREATE TABLE location (
                    processed_dttm timestamp ,
                    processed_dt date ,
                    "source" varchar(50) ,
                    location_lvl varchar(5), 
                    location_id varchar(30) ,
                    location_rk text PRIMARY KEY,
                    location_name varchar(100),
                    parent_location_id varchar(30),
                    parent_location_rk text
                );
              """
        )

    # Создаем таблицу содержащую справочную информацию о валюте - currency
    DBManager().adhoc_query(
              f"""
                DROP TABLE IF EXISTS currency CASCADE;
                CREATE TABLE currency (
                    processed_dttm timestamp,
                    processed_dt date,
                    "source" varchar(50),
                    currency_id varchar(30),
                    currency_rk text PRIMARY KEY,
                    currency_name varchar(50),
                    currency_rub_rate numeric(5, 2)
                );
              """
        )

    # Создаем таблицу содержащую информацию о работадателях - employer
    DBManager().adhoc_query(
              f"""
                DROP TABLE IF EXISTS employer CASCADE;
                CREATE TABLE employer (
                    processed_dttm timestamp,
                    processed_dt date,
                    "source" varchar(50),
                    employer_id varchar(30),
                    employer_rk text PRIMARY KEY,
                    employer_name varchar(100),
                    employer_url varchar(150),
                    location_rk TEXT,
                    FOREIGN KEY (location_rk) REFERENCES location(location_rk) ON DELETE CASCADE
                );
              """
        )

    # Создаем таблицу содержащую информацию о вакансиях - vacancy
    DBManager().adhoc_query(
              f"""
                DROP TABLE IF EXISTS vacancy CASCADE;
                CREATE TABLE vacancy (
                    processed_dt date ,
                    valid_from_dttm timestamp,
                    valid_to_dttm timestamp,
                    "source" varchar(50) ,
                    vacancy_id varchar(30) ,
                    vacancy_rk text ,
                    vacancy_name varchar(100) ,
                    vacancy_url varchar(200) ,
                    vacancy_sal_from int ,
                    vacancy_sal_to int ,
                    employer_rk text ,
                    currency_rk text,
                    location_rk text,	
                    vacancy_sal_gross_flg int4,
                    PRIMARY KEY (vacancy_rk, valid_from_dttm),
                    FOREIGN KEY (currency_rk) REFERENCES currency(currency_rk),
                    FOREIGN KEY (location_rk) REFERENCES location(location_rk),
                    FOREIGN KEY (employer_rk) REFERENCES employer(employer_rk)
                );
              """
        )

    # Создаем функцию для триггера. Функция будет использоваться в триггере для предотвращения вставки дубликатов в таблицу vacancy
    DBManager().adhoc_query(
              f"""
                CREATE OR REPLACE FUNCTION prevent_duplicate_insert()
                RETURNS TRIGGER AS $$
                BEGIN
                  -- Проверяем, совпадают ли вставляемые данные с данными в таблице
                  IF EXISTS (SELECT 1 FROM vacancy 
                                 WHERE vacancy_id = NEW.vacancy_id
                                 AND vacancy_rk = NEW.vacancy_rk
                                 AND vacancy_name = NEW.vacancy_name
                                 AND vacancy_url = NEW.vacancy_url
                                 AND (vacancy_sal_from = NEW.vacancy_sal_from OR (vacancy_sal_from IS NULL AND NEW.vacancy_sal_from IS NULL))
                                 AND (vacancy_sal_to = NEW.vacancy_sal_to OR (vacancy_sal_to IS NULL AND NEW.vacancy_sal_to IS NULL))
                                 AND (currency_rk = NEW.currency_rk OR (currency_rk IS NULL AND NEW.currency_rk IS NULL))
                                 AND location_rk = NEW.location_rk
                                 AND (vacancy_sal_gross_flg = NEW.vacancy_sal_gross_flg OR (vacancy_sal_gross_flg IS NULL AND NEW.vacancy_sal_gross_flg IS NULL))
                             ) THEN
                    -- Если есть совпадение, то пропускаем вставку
                    RETURN NULL;
                  ELSE
                    -- Если нет совпадения, то разрешаем вставку
                    RETURN NEW;
                  END IF;
                END;
                $$
                LANGUAGE plpgsql;
              """
        )

    # Создаем триггер на таблицу vacancy, на основе функции prevent_duplicate_insert()
    DBManager().adhoc_query(
              f"""
                CREATE OR REPLACE TRIGGER check_duplicates
                BEFORE INSERT ON vacancy
                FOR EACH ROW EXECUTE FUNCTION prevent_duplicate_insert();
              """
        )

if __name__ == '__main__':
    create_all_objects()