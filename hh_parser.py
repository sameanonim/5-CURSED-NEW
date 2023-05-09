import json
import time
import requests

class HeadHunter:
    # Константа для хранения базового URL API
    URL = f'https://api.hh.ru/vacancies'

    def __init__(self, search_keyword):
        # Инициализируем параметры запроса с ключевым словом поиска
        self.params = {'text': f'{search_keyword}',
                       'page': 0,
                       'per_page': 100}

    def get_request(self):
        # Отправляем GET-запрос к API и проверяем статус ответа
        response = requests.get(self.URL, self.params)
        if response.status_code == 200:
            # Преобразуем ответ в JSON-формат и возвращаем данные
            data = response.json()
            return data


def get_request_company(self):
    # Формируем URL для запроса по компаниям
    url = 'https://api.hh.ru/vacancies?text=' + search_keyword
    # Создаем пустой список для хранения идентификаторов компаний
    company_id = []
    # Цикл по 15 страницам результатов
    for item in range(15):
        # Получаем список вакансий по ключевому слову и преобразуем в JSON-формат
        request_hh = requests.get(url, params={"keywords": search_keyword}).json()['items']
        # Делаем паузу в полсекунды, чтобы не перегрузить сервер
        time.sleep(0.5)
        # Цикл по вакансиям на странице
        for item2 in request_hh:
            # Если список компаний достиг максимального размера, прерываем цикл
            if len(company_id) == 15:
                break
            # Добавляем идентификатор компании в список, если он еще не там
            company_id.add(item2['employer']['id'])
    # Возвращаем список идентификаторов компаний
    return company_id


def get_employers(self):
    # Создаем пустой список для хранения работодателей
    employers = []
    # Получаем данные от API с помощью метода get_request
    data = self.get_request()
    # Получаем список вакансий из данных
    items = data.get('items')
    # Цикл по первым 15 вакансиям
    for item in items[:15]:
        # Проверяем, что у вакансии есть идентификатор и название работодателя
        if item.get('employer').get('id') != 0 and item.get('employer').get('name') != 0:
            # Добавляем кортеж с идентификатором и названием работодателя в список
            employers.append((item.get('employer').get('id'),
                              item.get('employer').get('name')))
        # Удаляем дубликаты из списка работодателей с помощью преобразования в множество и обратно в список
        unigue_employers = list(set(employers))
    # Сохраняем список работодателей в файле JSON с красивым форматированием и поддержкой кириллицы
    with open('employers.json', 'w', encoding="UTF-8") as file:
        json.dump(unigue_employers, file, indent=4, ensure_ascii=False)
    # Возвращаем список работодателей
    return unigue_employers

@staticmethod
def get_info(data):
    # Получаем идентификатор вакансии из данных и преобразуем его в целое число
    vacancy_id = int(data.get('id'))
    # Получаем название вакансии из данных
    name = data['name']
    # Получаем идентификатор работодателя из данных и преобразуем его в целое число
    employer_id = int(data.get('employer').get('id'))
    # Получаем название города из данных
    city = data.get('area').get('name')
    # Получаем URL вакансии из данных
    url = data.get('alternate_url')

    # Инициализируем зарплату как None
    salary = None
    # Проверяем, есть ли у вакансии зарплата и валюта
    if data.get('salary') and data.get('salary').get('currency') == "RUR":
        # Если валюта рубли, то получаем нижнюю границу зарплаты из данных
        salary = data.get('salary').get('from')

    # Создаем кортеж с данными о вакансии
    vacancy = (vacancy_id, name, employer_id, city, salary, url)
    # Возвращаем кортеж
    return vacancy


def get_vacancies(self):
    # Создаем пустой список для хранения вакансий
    vacancies = []
    # Инициализируем номер страницы как 0
    page = 0
    # Запускаем бесконечный цикл
    while True:
        # Устанавливаем номер страницы в параметрах запроса
        self.params['page'] = page
        # Получаем данные от API с помощью метода get_request
        data = self.get_request()
        # Цикл по вакансиям в данных
        for vacancy in data.get('items'):
            # Проверяем, есть ли у вакансии зарплата и валюта
            if vacancy.get('salary') is not None and vacancy.get('salary').get('currency') is not None:
                # Если валюта рубли, то добавляем данные о вакансии в список с помощью метода get_info
                if vacancy.get('salary').get('currency') == "RUR":
                    vacancies.append(self.get_info(vacancy))
                else:
                    # Если валюта не рубли, то пропускаем вакансию
                    continue
            else:
                # Если у вакансии нет зарплаты или валюты, то добавляем данные о вакансии в список с помощью метода get_info
                vacancies.append(self.get_info(vacancy))

        # Увеличиваем номер страницы на 1
        page += 1
        # Делаем паузу в 0.2 секунды, чтобы не перегрузить сервер
        time.sleep(0.2)

        # Проверяем, достигли ли мы последней страницы результатов
        if data.get('pages') == page:
            # Если да, то прерываем цикл
            break
    # Сохраняем список вакансий в файле JSON с красивым форматированием и поддержкой кириллицы
    with open('vacancies.json', 'w', encoding="UTF-8") as file:
        json.dump(vacancies, file, indent=4, ensure_ascii=False)
    # Преобразуем список вакансий в список кортежей (необязательно)
    vacancy_lst = list(vacancies)
    # Возвращаем список вакансий
    return vacancies

if __name__ == '__main__':
    search_keyword = 'Python'
    hh = HeadHunter(search_keyword)
    k = hh.get_request()
    print(k)