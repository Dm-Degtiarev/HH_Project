import os

# Берем пароль из переменной среды 'PG_PASSWORD'
pg_password = os.getenv('PG_PASSWORD')

# Вводим данные для подключения к БД
host = 'localhost'
user = 'postgres'
password = pg_password
db_name = 'main'