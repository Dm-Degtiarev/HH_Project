# Проект: HH.ru

В рамках данного проекта реализуется реляционная база данных на основе СУБД "PostgreSQL", которая наполянется данными с сайта HH.ru, посредством API и Python.

**Архитектура БД:**

![архитектура БД](https://user-images.githubusercontent.com/123110865/233460105-7e5247c9-0077-422c-b0c3-7fddd16db7c6.png)

**Способы обновления таблиц в БД:**
1. **SCD1** - при вставке новых данных запускается проверка по ключу, если вставляемые данные полностью идентичны тем что уже есть в таблице - строка пропускается, а если какой-либо атрибут имеет отличное значение от старого, то строка перезаписывается.
2. **SCD2** - при вставке данных запускается проверка по ключу, если вставляемые данные полностью идентичны тем что уже есть в таблице - строка пропускается, а если какой-либо атрибут имеет отличное значение от старого - старая строка закрывается текущей датой и временем, а новая приобретает актуальность (**valid_to_dttm** = '9999-12-31'). Пример:

![пример историчности](https://user-images.githubusercontent.com/123110865/233473442-89029e31-0b92-4eab-8963-4bfdd2d4d4b1.png)

**Описание таблиц и их атрибутного состава:**

**Currency** - таблица содержащая информацию о валютах. Способ обновления - SCD1;

![image](https://user-images.githubusercontent.com/123110865/233463879-97e73185-a20f-4f5f-80ce-5bf1bc37dfd7.png)

**Location** - таблица содержащая информацию о местоположениях (локациях). Способ обновления - SCD1;

![image](https://user-images.githubusercontent.com/123110865/233466143-52bff06a-6295-434d-87d0-345b1568acf5.png)

**Employer** - таблица содержащая информацию о работодателях. Способ обновления - SCD1;

![image](https://user-images.githubusercontent.com/123110865/233468442-dc332a7b-3402-4e24-8f36-c51eb393de56.png)

**Vacancy** - таблица содержащая информацию о вакансиях. Способ обновления - SCD2;

![image](https://user-images.githubusercontent.com/123110865/233471440-9011aab9-a991-4e87-8e35-e0d72c77f322.png)

**Подготовка к работе:**
1. Необходимо установить проект в локальный репозиторий;
2. Необходимо наличие установленных на вашем компьютере языка программирования Python и системы управления базами данных PostgreSQL;
3. После установки проекта в локальный репозиторий, необходимо установить все модули файла зависимостей **requirements.txt**, воспользовавшийсь командой: **"pip install -r requirements.txt**";
4. В файле **/pg_utils/pg_config.py** необходимо заменить реквизиты подключения к БД на свои;
5. Запустить скрипт **/main/main.py** - скрипт создаст перечисленныее ранее таблицы, наполнит их данными с HH.ru, и при повторном запуске будет добавлять/изменять данные если были добавлены/изменены вакансии по которым идет поиск.

**FAQ:**
1. Пакет **pg_utils** содержит дополнительные утилиты работы с БД, а так же конфигурационный файл подключения к БД;
2. Пакет **classes** содержит модули: **engine** - для работы с API HH.ru, **dbmanager** - модуль работы с СУБД PostgreSQL;
3. Пакет **main** содержит модули: **create_tables** - создает вышеперечисленные таблицы и создает зависимости между ними, **insert_to_tables** - создает таблицы(если не созданы), и наполняет их данными с HH.ru, **main** - создает таблицы, добавляет в них данные, и демонстрирует успешную работу методов класса **DBManager**;
4. Бесплатная работа с API HH.ru (без токена) позволяет одновременно получить не более 2000 объектов, к примеру, вакансий;
5. Вставка данных идет по 10 компаниям, идентификаторы которых записаны в список **company_list**, расположенный в модуле **main.insert_to_tables**. Добавляя новые идентификаторы, можно получать и записывать инофрмацию о новых компаниях. Идентификатор компании можно узнать из адресной строки браузера, находясь на странице компании. Пример: https://hh.ru/employer/2624085;
6. В директории **sql** расположен sql-код, который используется в работе программы. **create_objects.sql** - создание объектов, **DBManager.sql** - запросы для методов класса **DBManager**.

Более детальное описание, по каждому этапу, находится в комментариях кода.





