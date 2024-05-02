from connect import DBConnect


def change_wp_name():
    wp_name = input("Введите название рабочего места: ")

    # Get work by WP
    select_wp = f"""
    select w."text" work_name,
           w.id
    from lab.work as w,
         lab.workplace as wp
    where wp."text" = '{wp_name}'
        and w.workplace_id = wp.id
    """

    connection = DBConnect.connect
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
        new_work = work[:current_left_brekit:] + wp_name + ']'
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

    return "Успех!"


if __name__ == '__main__':
    change_wp_name()
    print('Скрипт закончил работу! \nВсе изменения применены, обратного пути нет!')
