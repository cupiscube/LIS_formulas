#-*- coding: UTF-8 -*-

# from sys import argv
import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# Дабы в консоль не выводилось предупреждение
import warnings
warnings.filterwarnings("ignore", 'This pattern has match groups')
warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression')


# wp_name, work_name = argv
wp_name = None
work_name = None

host = input('Введите IP адрес сервера: ')
port = input('Введите порт базы постгрес (если порт 5433 можно пропустить нажав Enter): ')
if port == '':
    port = 5433
else:
    port = int(port)

db_name = input('Введите название БД (если БД bregis_lis данный шаг можно пропустить нажав Enter): ')
if db_name == '':
    db_name = 'bregis_lis'

print('Данный скрипт чинит легкие формулы в результатах. \n '
      'Соотносит названия результатов по обозначениям которые вы укажете ниже.')
print('Если вы хотите починить все формулы во всех работах в рамках одного рабочего места, '
      'введите название РМ ниже, иначе пропустите шаг нажав Enter. \n'
      'Скрипт предложит ввести название конкретной рабочты (название работы должно быть уникальным).')
wp_name = input("Введите название рабочего места: ")

if wp_name == '':
    work_name = input("Введите название рабочего места: ")

db_user = 'lis_admin'
db_password = 'Bre91s_Admin'

db_url = f"host={host} port={port} dbname={db_name} user={db_user} password={db_password}"
connection = pg.connect(db_url)

if wp_name is not None:
    select_works = f"""
                    select w."text" work_name
                    from lab.work as w,
                         lab.workplace as wp
                    where wp."text" = '{wp_name}'
                        and w.workplace_id = wp.id
                    """
    cursor = connection.cursor()
    cursor.execute(select_works)
    wp = cursor.fetchall()
    works = [w[0] for w in wp]
    cursor.close()
else:
    works = [work_name]

url = URL.create(
    drivername="postgresql",
    username=db_user,
    password=db_password,
    host=host,
    port=port,
    database=db_name
)
engine = create_engine(url)
# connection = engine.connect()


def app():
    ident_1 = input('Вевдите название идентификатора для обозначения результата с формулами, например "(числовой)": ')
    ident_2 = input(
        'Вевдите название идентификатора для обозначения результата с анализатора, например "(анализатор)": ')

    for work in works:
        # Get data
        cursor = connection.cursor()
        select = f"""
                    select wr."text" result_name, wrv.*
                    from lab.work as w,
                       lab.workresult as wr,
                       lab.workresult_value as wrv
                    where w."text" = '{work}'
                        and wr.work_id = w.id
                        and wrv.workresult_id = wr.id
                    """
        cursor.execute(select)
        dataframe = psql.read_sql(select, engine)
        cursor.close()
        # dataframe = psql.read_sql(select, db_url)
        dataframe['result_name'] = dataframe['result_name'].str.lower()
        # Get data with function
        func_df = dataframe.dropna(subset=['func'])
        func_df = func_df[['result_name', 'id', 'func', 'text']]

        df = dataframe.loc[(dataframe['result_name'].isin(func_df['result_name']))]

        # Get values
        names = df['result_name'].str.replace(ident_1, '')
        target_values = dataframe[dataframe['result_name'].str.contains(ident_1)]
        analizator_values = dataframe[dataframe['result_name'].str.contains(ident_2)]

        # Select values
        def get_values(values, ident):
            values_without_ident = values['result_name'].str.replace(ident, '')
            values.loc[:, ('result_name')] = values_without_ident
            return values

        target_values = get_values(target_values, ident_1)
        analizator_values = get_values(analizator_values, ident_2)

        # Change functions
        target_names = target_values['result_name'].tolist()

        for name in target_names:
            idx_anal = analizator_values.loc[analizator_values['result_name'] == name]['id'].tolist()[0]
            target_func = target_values.loc[target_values['result_name'] == name]['func'].tolist()[0]
            # find all values in func
            i = 0
            value_indxs = []
            while i != -1:
                # print(target_func[sum(value_indxs)::])
                try:
                    i = target_func[sum(value_indxs) + 1::].find('#')
                except:
                    i = -1
                    print(f'Внимание! Ошибка! Результат {name} в работе {work} не обновлен!!!')
                    print(target_func)
                value_indxs.append(i + 1)
            # last element is bad (-1 + 1 = 0)
            value_indxs.pop(-1)
            # replace all values in func on new values
            last_i = 0
            for n in value_indxs:
                last_i += n
                # tf = target_func[:last_i + 1 + 1:] + str(idx_anal) + target_func[last_i + 1 + 1 + 4::]
                target_func = target_func[:last_i + 1:] + str(idx_anal) + target_func[last_i + 1 + 4::]
                target_values.loc[target_values['result_name'] == name, 'func'] = target_func

        # Update
        cur = connection.cursor()
        ind = target_values.index
        for i in ind:
            v = target_values.loc[i]
            table_modification = f"""UPDATE lab.workresult_value as wrv SET func = '{str(v['func'])}' WHERE id = {int(v['id'])};"""
            # print(table_modification)
            try:
                cur.execute(table_modification)
                connection.commit()
            except:
                print('Except!')
                cur.execute("ROLLBACK")
                connection.commit()
        cur.close()
        print(f'Работа {work} обновлена.')


if __name__ == '__main__':
    app()
    print('Скрипт закончил работу! \nВсе изменения применены, обратного пути нет!')
