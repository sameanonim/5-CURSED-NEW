from configparser import ConfigParser


def config(filename="database.ini", section="postgresql"):
    # Создаем объект парсера конфигурации
    parser = ConfigParser()
    # Читаем файл конфигурации
    parser.read(filename)
    # Создаем словарь для хранения параметров подключения к базе данных
    db = {}
    # Проверяем, есть ли в файле нужная секция
    if parser.has_section(section):
        # Получаем список пар ключ-значение из секции
        params = parser.items(section)
        # Добавляем каждый параметр в словарь
        for param in params:
            db[param[0]] = param[1]
    else:
        # Если секции нет, выбрасываем исключение
        raise Exception('Section {0} is not found in the {1} file.'.format(section, filename))

    # Возвращаем словарь с параметрами подключения
    return db