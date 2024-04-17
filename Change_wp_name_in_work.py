import psycopg2 as pg
import pandas as pd
import pandas.io.sql as psql

# Get connection datas
host = input('Введите IP адрес сервера: ')
port = input('Введите порт базы постгрес (если порт 5433 можно пропустить нажав Enter): ')
if port == '':
    port = 5433
else:
    port = int(port)

db_name = input('Введите название БД (если БД bregis_lis данный шаг можно пропустить нажав Enter): ')
if db_name == '':
    db_name = 'bregis_lis'

print('Данный скрипт чинит названия работ в [] в одном рабочем месте')

wp_name = input("Введите название рабочего места: ")

# Get work by WP

connection = pg.connect(f"host={host} port={port} dbname={db_name} user=lis_admin password=Bre91s_Admin")
select_wp = f"""
select w."text" work_name,
       w.id
from lab.work as w,
	 lab.workplace as wp
where wp."text" = '{wp_name}'
    and w.workplace_id = wp.id
"""
# dataframe_wp = psql.read_sql(select, connection)

cursor = connection.cursor()

cursor.execute(select_wp)
wp = cursor.fetchall()
works = [w[0] for w in wp]

# Fix WP names
new_works_name = []
for work_ in wp:
    work = work_[0]
    # find left brekits
    current_left_brekit = work.find('[') + 1
    # current_left_brekit = 0
    # next_left_brekit = 0
    # left_brekits = []
    # print(work)
    # while next_left_brekit != -1:
    #     next_left_brekit = work[current_left_brekit+1::].find('[')
    #     if next_left_brekit != -1:
    #         current_left_brekit += next_left_brekit + 1
    #         left_brekits.append(current_left_brekit)
    # print(left_brekits)
    # find the last right brekit
    current_right_brekit = work[::].rfind(']')
    new_work = work[:current_left_brekit:] + '[' + wp_name + ']'
    new_works_name.append([new_work, work_[1]])

# Update values
cursor = connection.cursor()
for n_work in new_works_name:
    table_modification = f"""UPDATE lab.work as w SET text = '{str(n_work[0])}' WHERE id = {n_work[1]};"""
    # print(table_modification)
    try:
        cursor.execute(table_modification)
        connection.commit()
    except:
        print('Except!')
        cursor.execute("ROLLBACK")
        connection.commit()

print('Script finished!')
