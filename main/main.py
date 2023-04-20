from insert_to_tables import insert_to_tables
from classes.dbmanager import DBManager


if __name__ == '__main__':
    # Создаем таблицы и прочие объекты (если не созданы), и наполняем их данными
    insert_to_tables()

    # Демонтрируем работу методов класса DBManager
    print(f"-----Список компаний c кол-вом вакансий:\n{DBManager().get_companies_and_vacancies_count()}\n")
    print(f"-----Список вакансий:\n{DBManager().get_all_vacancies()}\n")
    print(f"-----Средняя ЗП по вакансиям:\n{DBManager().get_avg_salary()}\n")
    print(f"-----Список вакансий с ЗП выше средней:\n{DBManager().get_vacancies_with_higher_salary()}\n")
    print(f"-----Список вакансий по ключевому слову:\n{DBManager().get_vacancies_with_keyword('Analyst')}\n")