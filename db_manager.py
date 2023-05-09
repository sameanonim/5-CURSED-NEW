import json
import psycopg2
from hh_parser import HeadHunter
from psycopg2 import errors
from config import config


class DBManager:
    def __init__(self, dbname, params):
        # Инициализируем атрибуты класса с названием базы данных и параметрами подключения
        self.dbname = dbname
        self.params = params

    def create_database(self):
        """Создает базу данных и инициирует подключение к ней с указанными параметрами"""
        # Подключаемся к базе данных postgres с помощью библиотеки psycopg2
        conn = psycopg2.connect(dbname='postgres', **self.params)
        # Включаем автоматическое подтверждение транзакций
        conn.autocommit = True
        # Создаем объект курсора для выполнения SQL-запросов
        cur = conn.cursor()

        try:
            # Пытаемся удалить базу данных с заданным названием, если она уже существует
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")  # затирает БД если она уже существует
            # Пытаемся создать новую базу данных с заданным названием
            cur.execute(f"CREATE DATABASE {self.dbname}")  # создает новую БД
        except psycopg2.errors.ObjectInUse:
            # Если база данных используется другим процессом, то прерываем его с помощью функции pg_terminate_backend
            cur.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) "
                        "FROM pg_stat_activity "
                        f"WHERE pg_stat_activity.datname = '{self.dbname}' ")
            # Повторяем попытку удалить и создать базу данных
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
            cur.execute(f"CREATE DATABASE {self.dbname}")

        finally:
            # Закрываем курсор и соединение с базой данных postgres
            cur.close()
            conn.close()

        # Подключаемся к созданной базе данных с помощью библиотеки psycopg2
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        # Используем менеджеры контекста для автоматического закрытия соединения и курсора
        with conn:
            with conn.cursor() as cur:
                # Создаем таблицу employers с двумя столбцами: идентификатором и названием работодателя
                # Устанавливаем идентификатор как первичный ключ и название как уникальное значение
                cur.execute('CREATE TABLE IF NOT EXISTS employers '
                            '('
                            'employer_id int PRIMARY KEY, '
                            'employer_name varchar(255) UNIQUE NOT NULL)')
                # Создаем таблицу vacancies с семью столбцами: идентификатором, названием, идентификатором работодателя,
                # городом, минимальной и максимальной зарплатой и URL вакансии
                # Устанавливаем идентификатор как первичный ключ и ссылку на таблицу employers по идентификатору работодателя
                cur.execute('CREATE TABLE IF NOT EXISTS vacancies '
                            '('
                            'vacancy_id int PRIMARY KEY, '
                            'vacancy_name varchar(255) NOT NULL, '
                            'employer_id int REFERENCES employers(employer_id) NOT NULL, '
                            'city text, '
                            'salary_min int,'
                            'salary_max int,'
                            'url text)')
        # Закрываем соединение с базой данных (необязательно, так как менеджер контекста это делает автоматически)
        conn.close()

    def insert_data_into_db(self, data):
        """Подключается к БД и заполняет таблицы данными из запроса"""
        # Подключаемся к базе данных с помощью библиотеки psycopg2
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        # Используем менеджеры контекста для автоматического закрытия соединения и курсора
        with conn:
            with conn.cursor() as cur:
                # Включаем автоматическое подтверждение транзакций
                conn.autocommit = True
                # Создаем объект курсора для выполнения SQL-запросов
                cur = conn.cursor()
                # Цикл по элементам данных из запроса
                for item in data['items']:
                    # Получаем данные о работодателе из элемента
                    employer = item['employer']
                    # Выполняем SQL-запрос для вставки данных о работодателе в таблицу employers
                    # Используем ON CONFLICT DO NOTHING для предотвращения дублирования данных
                    cur.execute("""
                        INSERT INTO employers (employer_id, employer_name)
                        VALUES (%s, %s)
                        ON CONFLICT (employer_id) DO NOTHING;
                    """, (employer['id'], employer['name']))
                    # Подтверждаем транзакцию
                    conn.commit()

                    # Проверяем, есть ли у элемента данные о зарплате
                    if item.get('salary') is not None:
                        # Если да, то получаем нижнюю и верхнюю границу зарплаты из элемента
                        salary_from = item.get('salary').get('from')
                        salary_to = item.get('salary').get('to')
                    else:
                        # Если нет, то устанавливаем зарплату как None
                        salary_from = None
                        salary_to = None

                    # Выполняем SQL-запрос для вставки данных о вакансии в таблицу vacancies
                    # Используем ON CONFLICT DO NOTHING для предотвращения дублирования данных
                    cur.execute("""
                        INSERT INTO vacancies (vacancy_id, vacancy_name, employer_id, city, salary_min, salary_max, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (vacancy_id) DO NOTHING;
                    """, (item['id'], item['name'], item['employer']['id'], json.dumps(item['address']), salary_from,
                          salary_to, item['url']))
                    # Подтверждаем транзакцию
                    conn.commit()

    def _execute_query(self, query) -> list:
        """Возвращает результат запроса"""
        # Подключаемся к базе данных с помощью библиотеки psycopg2
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        try:
            # Используем менеджеры контекста для автоматического закрытия соединения и курсора
            with conn:
                with conn.cursor() as cur:
                    # Выполняем SQL-запрос и получаем результат
                    cur.execute(query)
                    result = cur.fetchall()

        finally:
            # Закрываем соединение с базой данных (необязательно, так как менеджер контекста это делает автоматически)
            conn.close()

        # Возвращаем результат запроса в виде списка кортежей
        return result

    def get_companies_and_vacancies_count(self) -> list:
        # Выполняем SQL-запрос для получения названий компаний и количества вакансий по каждой компании
        # Используем LEFT JOIN для объединения таблиц vacancies и employers по идентификатору работодателя
        # Используем GROUP BY для группировки результатов по названию компании
        # Используем ORDER BY для сортировки результатов по убыванию количества вакансий и по алфавиту названия компаний
        result = self._execute_query("SELECT employer_name, COUNT(*) as quantity_vacancies "
                                     "FROM vacancies "
                                     "LEFT JOIN employers USING(employer_id)"
                                     "GROUP BY employer_name "
                                     "ORDER BY quantity_vacancies DESC, employer_name")
        # Возвращаем результат запроса в виде списка кортежей
        return result

    def get_all_vacancies(self) -> list:
        # Выполняем SQL-запрос для получения названий компаний, названий вакансий, максимальной зарплаты и URL вакансий
        # Используем JOIN для объединения таблиц vacancies и employers по идентификатору работодателя
        # Используем WHERE для фильтрации результатов по наличию максимальной зарплаты
        # Используем ORDER BY для сортировки результатов по убыванию максимальной зарплаты и по алфавиту названия вакансий
        result = self._execute_query("SELECT employers.employer_name, vacancy_name, salary_max, url "
                                     "FROM vacancies "
                                     "JOIN employers USING(employer_id)"
                                     "WHERE salary_max IS NOT NULL "
                                     "ORDER BY salary_max DESC, vacancy_name")
        # Возвращаем результат запроса в виде списка кортежей
        return result

    def get_avg_salary(self) -> list:
        # Выполняем SQL-запрос для получения среднего значения максимальной зарплаты по всем вакансиям
        # Используем функцию AVG для вычисления среднего арифметического
        # Используем функцию ROUND для округления результата до целого числа
        result = self._execute_query("SELECT ROUND(AVG(salary_max)) as average_salary "
                                     "FROM vacancies")
        # Возвращаем результат запроса в виде списка кортежей
        return result

    def get_vacancies_with_higher_salary(self) -> list:
        # Выполняем SQL-запрос для получения названий вакансий и максимальной зарплаты по вакансиям с зарплатой выше средней
        # Используем WHERE для фильтрации результатов по условию salary_max > (SELECT AVG(salary_max) FROM vacancies)
        # Используем ORDER BY для сортировки результатов по убыванию максимальной зарплаты и по алфавиту названия вакансий
        result = self._execute_query("SELECT vacancy_name, salary_max "
                                     "FROM vacancies "
                                     "WHERE salary_max > (SELECT AVG(salary_max) FROM vacancies) "
                                     "ORDER BY salary_max DESC, vacancy_name")
        # Возвращаем результат запроса в виде списка кортежей
        return result

    def get_vacancies_with_keyword(self, word: str) -> list:
        # Выполняем SQL-запрос для получения названий вакансий, содержащих заданное слово
        # Используем WHERE для фильтрации результатов по условию vacancy_name ILIKE '%{word}%'
        # ILIKE означает регистронезависимое сравнение строк с использованием шаблона
        # % означает любое количество любых символов
        result = self._execute_query("SELECT vacancy_name "
                                     "FROM vacancies "
                                     f"WHERE vacancy_name ILIKE '%{word}%'"
                                     "ORDER BY vacancy_name")
        # Возвращаем результат запроса в виде списка кортежей
        return result

if __name__ == '__main__':
    # Получаем параметры подключения к базе данных из файла config.py
    params = config()
    # Создаем объект класса DBManager с названием базы данных и параметрами подключения
    db = DBManager('head_hunter', params)
    # Вызываем метод get_companies_and_vacancies_count и выводим результат на экран
    data = db.get_companies_and_vacancies_count()
    print(data)
    # Вызываем метод get_all_vacancies и выводим результат на экран
    data_2 = db.get_all_vacancies()
    print(data_2)
    # Вызываем метод get_avg_salary и выводим результат на экран
    data_3 = db.get_avg_salary()
    print(data_3)
    # Вызываем метод get_vacancies_with_higher_salary и выводим результат на экран
    data_4 = db.get_vacancies_with_higher_salary()
    print(data_4)
    # Вызываем метод get_vacancies_with_keyword с аргументом 'python' и выводим результат на экран
    data_5 = db.get_vacancies_with_keyword('python')
    print(data_5)