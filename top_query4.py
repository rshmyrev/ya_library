#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
'''
Создание топа запросов (строк).
Можно сделать отдельные топы по регионам.

Берёт данные из папки path_in (по умолчанию "Data").
Готовые файлы кладёт в path_out (по умолчанию "Output/Топ запросов").
Для работы нужно перечислить список файлов (names)
и расширение (ext).
Есть возможность отсеивать строки по регулярному выражению "anti_reg_text"
и указывать исключения из регулярного выражения "anti_reg_exception_text".
'''

import os
import time
import csv
import re


# Функция преобразует время из формата UNIX в читабельный
def t(UNIX=None):
    return '%s' % (time.strftime("%d %b %Y %H:%M:%S", time.localtime(UNIX)))


# Функция создаёт словарь трансформации: {старое значение: новое значение}.
# Данные берёт из внешнего файла.
def build_transform_dict(name, encoding, delimiter, col_old_key, col_new_key):
    transform_dict = {}  # создаём словарь трансформации
    # открываем внешний файл с парами "старое значение - новое значение"
    with open('%s' % name, 'r', encoding=encoding) as key_file:
        # открываем файл, как csv (с разбивкой по столбцам)
        csv_key_file = csv.reader(key_file, delimiter=delimiter)
        # читаем файл построчно
        for row in csv_key_file:
            # добавляем в словарь значения старого и нового ключей
            transform_dict[row[col_old_key - 1]] = row[col_new_key - 1]
    return transform_dict  # возвращаем словарь трансформации


# Функция преобразует старое значение в новое, используя словарь transform_dict
def transform(key, transform_dict, key_default):
    try:  # берём значение ключа из словаря
        key = transform_dict[key]
    except:  # если в словаре нет нужного ключа
        print('%s\tKey "%s" not in dict' % (t(), key))
        key = key_default  # присваиваем значение по умолчанию
    finally:
        return key


# Функция проверяет строку на соответствие другой заданной строке
# или на срабатывание регулярного выражения
def check(query, check_type, **args):
    if check_type == 'full':  # проверка на полное соответствие
        query_list = args.get('query_list')
        if query in query_list:
            return True
    if check_type == 'reg':  # проверка регулярными выражениями
        reg = args.get('reg')
        reg_exception = args.get('reg_exception')
        if reg.search(query):  # если сработала регулярка
            # и нет исключения или оно не сработало
            if not (reg_exception and reg_exception.search(query)):
                return True


# Функция сортирует словарь запросов по убыванию к-ва и записывает в файл
def dict_in_file(path_out, name, ext, encoding, query_dict):
    # создаём папку path_out, если не существует
    if not os.path.exists(path_out):
        os.makedirs(path_out)
    file_w = open('%s/%s' % (path_out, name + ext), 'w', encoding=encoding)
    # сортируем словарь запросов и записываем в файл
    count_list = sorted(query_dict.items(), key=lambda x: (x[1] * (-1), x[0]))
    for item, value in count_list:
        s = str("{0}\t{1}\n").format(value, item)
        file_w.write(s)
    file_w.close()  # закрываем файл


# Функция создаёт топ запросов. Можно создать топ отдельных частей (слов).
# Есть возможность выполнять проверку строк.
# Топы можно разбить по отдельным файлам, используя для разбиения ключи.
def top(names=['input'], path_in='Data', path_out='Output', **pargs):
    # пишем время, имя модуля и функции
    print('%s\tStart script %s.top' % (t(), __name__))

    # извлекаем параметры настройки работы скрипта
    ext = pargs.get('extension', '.txt')
    encoding = pargs.get('encoding', 'utf-8')
    delimiter = pargs.get('delimiter', '\t')
    col_query = pargs.get('col_query', 1)
    col_count = pargs.get('col_count', False)
    lower = pargs.get('lower', True)
    separator = pargs.get('separator', False)
    compliance_test = pargs.get('compliance_test', False)
    split_by_key = pargs.get('split_by_key', False)
    col_key = pargs.get('col_key', False)
    transform_key = pargs.get('transform_key', False)
    req_per_day = pargs.get('req_per_day', False)

    # Если нужно разбить топ по отдельным файлам, используя для разбиения
    # ключи из какого-либо столбца, и эти ключи нужно ещё преобразовать,
    # то создаём словарь преобразования вида {старое значение: новое значение}.
    if transform_key:
        # извлекаем параметры трансформации и создаём из них словарь
        transform_param = {
            'name': pargs.get('key_file', 'ID_Regions.txt'),
            'encoding': pargs.get('key_file_encoding', 'utf-8'),
            'delimiter': pargs.get('key_file_delimiter', '\t'),
            'col_old_key': pargs.get('col_old_key', 1),
            'col_new_key': pargs.get('col_new_key', 2),
        }
        # вызываем функцию создания словаря и передаём ей в качестве
        # именованных аргументов словарь параметров трансформации
        transform_dict = build_transform_dict(**transform_param)

    # Если нужна проверка на соответствие строки другой заданной строке
    # или регулярному выражению
    if compliance_test:
        # извлекаем параметры проверки
        check_type = pargs.get('check_type', False)
        query_list = pargs.get('query_list', False)
        reg_text = pargs.get('reg_text', False)
        reg_exception_text = pargs.get('reg_exception_text', False)
        action = pargs.get('action', False)

        # если необходимо, создаём объекты регулярных выражений
        if reg_text:  # есть регулярка
            reg = re.compile(reg_text, re.I)
        else:
            reg = False
        if reg_exception_text:  # есть исключения из регулярки
            reg_exception = re.compile(reg_exception_text, re.I)
        else:
            reg_exception = False

    # Обходим в цикле файлы из списка
    for name in names:
        # открываем файл из списка
        file_r = open('%s/%s' % (path_in, name + ext), 'r', encoding=encoding)
        # открываем файл, как csv (с разбивкой по столбцам)
        csv_r = csv.reader(file_r, delimiter=delimiter, quoting=csv.QUOTE_NONE)

        total_line = 0  # счётчик обработанных строк
        query_dict = {}  # создаём пустой словарь запросов

        # читаем файл построчно
        for row in csv_r:
            total_line += 1  # увеличиваем счётчик строк
            query = row[col_query - 1]  # берём запрос из соотв. столбца
            if col_count:  # если есть столбец с количеством запросов
                count = int(row[col_count - 1])  # берём кол-во из него
            else:  # иначе считаем, что в каждой строке один запрос
                count = 1

            # если нужно, преобразуем запрос в строчные буквы
            if lower:
                query = query.lower()
            
            #если необходимо подсчитать количество запросов по дням
            if req_per_day:            
                query = time.strftime("%d.%m.%Y", time.localtime(int(query)))

            # если необходимо работать с частями запроса (например, словами)
            if separator:
                # разбивам запрос на части по разделителю
                parts = re.split(separator, query)
            else:
                # в противном случае в списке будет 1 часть - исходный запрос
                parts = []
                parts.append(query)

            # Анализируем каждую часть запроса.
            # Если разбивки не было, то запрос целиком.
            for query in parts:
                # если нужно, то проверяем запрос
                if compliance_test:
                    # создаём словарь параметров проверки
                    check_param = {
                        'query': query,
                        'check_type': check_type,
                        'query_list': query_list,
                        'reg': reg,
                        'reg_exception': reg_exception,
                    }
                    # вызываем функцию проверки
                    check_res = check(**check_param)
                    # если нужно удалить прошедшие проверку строки
                    if action == 'delete' and check_res:
                        continue  # пропускаем строку
                    # если нужно удалить не прошедшие проверку строки
                    elif action == 'save' and not check_res:
                        continue  # пропускаем строку

                # Если разбивка по файлам не нужна, то делаем общий топ и
                # записываем запросы в словарь query_dict.
                # В противном случае делаем отдельные топы и записываем запросы
                # в словарь query_dict по ключам.
                if not split_by_key:
                    # если запроса нет в словаре, то добавляем
                    if query not in query_dict:
                        query_dict[query] = count
                    # если запрос уже есть в словаре, увеличиваем счётчик
                    else:
                        query_dict[query] += count

                else:
                    key = row[col_key - 1]  # берем ключ из указанного столбца
                    if transform_key:  # если ключи нужно преобразовать
                        key_default = pargs.get('key_default', '!KeyNotInDict')
                        key = transform(key, transform_dict, key_default)
                    # если ключа нет в словаре, то добавляем
                    if key not in query_dict:
                        query_dict[key] = {query: count}
                    # если запроса нет в словаре ключа, то добавляем
                    elif query not in query_dict[key]:
                        query_dict[key][query] = count
                    # если запрос уже есть в словаре ключа, увеличиваем счётчик
                    else:
                        query_dict[key][query] += count

        file_r.close()  # закрываем файл

        # Теперь создан словарь (либо общий, либо по ключам).
        # Если разбивка по файлам не нужна, то записываем общий топ.
        if not split_by_key:
            dict_in_file(path_out, name, ext, encoding, query_dict)
        # Если нужна, то для каждого ключа записываем свой топ.
        else:
            # файлы будем складывать в папку с названием исходного файла
            path_out = path_out + '/' + name
            # пройдёмся по ключам в словаре и каждый запишем в файл
            for key in query_dict:
                dict_in_file(path_out, key, ext, encoding, query_dict[key])

        print('%s\tDone file "%s". Total line: %d' % (t(), name, total_line))
    print('%s\tFinish script %s.top%s' % (t(), __name__, '\n' + '-' * 50))


if __name__ == '__main__':
    top()
