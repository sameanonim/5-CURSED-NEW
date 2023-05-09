# импортируем модуль для работы с базой данных
from db_manager import DBManager
# импортируем модуль с настройками
from config import config
# импортируем модуль для парсинга сайта hh.ru
from hh_parser import HeadHunter

# задаем пути к файлам с данными
PATH = 'vacancies.json'
PATH_2 = 'employers.json'

# определяем основную функцию
def main():
    # получаем параметры для подключения к базе данных
    params = config()
    # создаем объект класса DBManager с именем базы данных и параметрами
    db = DBManager('head_hunter', params)
    print('Создаем БД и таблицы')
    # вызываем метод create_database для создания базы данных и таблиц
    db2 = db.create_database()
    print(f'БД и таблицы созданы')
    # задаем ключевое слово для поиска вакансий
    search_keyword = 'Python'
    # создаем объект класса HeadHunter с ключевым словом
    hh = HeadHunter(search_keyword)
    # вызываем метод get_request для получения данных с сайта hh.ru
    k = hh.get_request()
    print('Добавляем данные из запроса в БД')
    # вызываем метод insert_data_into_db для добавления данных в базу данных
    db.insert_data_into_db(k)
    print("""Получаем список компаний и количество вакансий""")
    # вызываем метод get_companies_and_vacancies_count для получения списка компаний и количества вакансий у каждой из них
    data = db.get_companies_and_vacancies_count()
    print(data)
    print("""Получаем список всех вакансий""")
    # вызываем метод get_all_vacancies для получения списка всех вакансий
    data_2 = db.get_all_vacancies()
    print(data_2)
    print("""Получает среднюю зарплату по вакансиям""")
    # вызываем метод get_avg_salary для получения средней зарплаты по вакансиям
    data_3 = db.get_avg_salary()
    print(data_3)
    print("""Получаем список всех компаний и количество вакансий у каждой компании""")
    # вызываем метод get_vacancies_with_higher_salary для получения списка вакансий с зарплатой выше средней
    data_4 = db.get_vacancies_with_higher_salary()
    print(data_4)
    print("""Получаем список всех компаний и количество вакансий у каждой компании""")
    # вызываем метод get_vacancies_with_keyword для получения списка вакансий, содержащих определенное ключевое слово
    data_5 = db.get_vacancies_with_keyword('python')
    print(data_5)

# проверяем, что модуль запускается как главный файл, а не импортируется
if __name__ == '__main__':
    # вызываем основную функцию
    main()