import sys
import psycopg2


# Функция, создающая структуру БД (таблицы)
def create_db(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 
                FROM pg_catalog.pg_database 
                WHERE datname = 'clients_db'""")
            exists = cur.fetchone()
            if exists is None:
                return False

            # удаление таблиц
            cur.execute("""
                DROP TABLE IF EXISTS phone;
                DROP TABLE IF EXISTS client;        
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS client(
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(40) NOT NULL,
                    last_name VARCHAR(40) NOT NULL,
                    email VARCHAR(40) NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS phone(
                    client_id INTEGER NOT NULL REFERENCES client(id),
                    phone VARCHAR(40) NOT NULL UNIQUE
                );
            """)
        print('База данных создана')
        conn.commit()  # фиксируем в БД
        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая добавить нового клиента
def add_client(conn, first_name, last_name, email, phones=None):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO client(first_name, last_name, email) 
                VALUES(%s, %s, %s) RETURNING id;
            """, (first_name, last_name, email, ))
            conn.commit()  # фиксируем в БД
            client_id = cur.fetchone()
            if phones:
                if type(phones) is str:
                    phones = phones.strip().split(',')
                    for phone in phones:
                        add_phone(conn, client_id, phone)
        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая добавить телефон для существующего клиента
def add_phone(conn, client_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO phone(client_id, phone) 
                VALUES(%s, %s);
            """, (client_id, phone.strip(),))
        conn.commit()  # фиксируем в БД
        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая изменить данные о клиенте
def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    try:
        # Обновляем данные клиента
        column = []
        param = []
        with conn.cursor() as cur:
            if first_name:
                column.append('first_name=%s')
                param.append(first_name)
            if last_name:
                column.append('last_name=%s')
                param.append(last_name)
            if email:
                column.append('email=%s')
                param.append(email)

            param.append(client_id)
            cur.execute(f"""
                UPDATE client
                SET {','.join(column)}
                WHERE id=%s
            """, param)

        # Обновляем данные телефонов
        if phones:
            delete_phone_all(conn, client_id)
            if type(phones) is str:
                phones = phones.strip().split(',')
                for phone in phones:
                    add_phone(conn, client_id, phone)

        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая удалить телефон для существующего клиента
def delete_phone(conn, client_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("""
            DELETE 
            FROM phone 
            WHERE client_id=%s and phone=%s;
            """, (client_id, phone,))
        conn.commit()  # фиксируем в БД
        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая удалить все телефоны для существующего клиента
def delete_phone_all(conn, client_id):
    try:
        with conn.cursor() as cur:
            cur.execute("""
            DELETE 
            FROM phone 
            WHERE client_id=%s;
            """, (client_id,))
        conn.commit()  # фиксируем в БД
        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id):
    try:
        delete_phone_all(conn, client_id)
        with conn.cursor() as cur:
            cur.execute("""
            DELETE 
            FROM client 
            WHERE id=%s;
            """, (client_id, ))
        return True, None
    except Exception as e:
        return False, e


# Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    try:
        column = []
        param = []
        with conn.cursor() as cur:
            if first_name:
                column.append('first_name=%s')
                param.append(first_name)
            if last_name:
                column.append('last_name=%s')
                param.append(last_name)
            if email:
                column.append('email=%s')
                param.append(email)
            if phone:
                column.append('phone=%s')
                param.append(phone)

            cur.execute(f"""
            SELECT id, first_name, last_name, email, phone
            FROM client as c LEFT JOIN phone as p ON (c.id = p.client_id)             
            WHERE {' and '.join(column)}
            """, param)

            return cur.fetchone(), None
    except Exception as e:
        return False, e


with psycopg2.connect(database="clients_db", user="postgres", password="qsDMgUQ0") as conn:
    if not create_db(conn)[0]:
        print('База данных clients_db не создана')
        sys.exit()

    # Загружаем новых клиентов
    clients = [
        ['Якуткина', 'Ирина', '+7 (967) 561-68-69, +7 (977) 851-89-62', 'irina1190@gmail.com'],
        ['Драчёва', 'Настасья', '+7 (912) 250-28-41', 'nastasya59@hotmail.com'],
        ['Щербинин', 'Феликс', '+7 (979) 719-94-13', 'feliks1960@outlook.com'],
        ['Канадов', 'Иннокентий', ['+7 (934) 404-64-15', '+7 (940) 652-45-87'], 'innokentiy17031971@gmail.com'],
        ['Сазонтов', 'Кирилл', '+7 (958) 163-91-54', 'kirill11111978@ya.ru'],
        ['Никанорова', 'Милана', '+7 (977) 441-85-25', 'milana8702@outlook.com'],
        ['Еремеев', 'Тимофей', '+7 (989) 686-86-83, +7 (953) 344-23-19', 'timofey1982@mail.ru'],
        ['Жутов', 'Давид', '+7 (972) 507-63-27', 'david82@mail.ru'],
        ['Бебчук', 'Мила', '+7 (915) 373-83-49', 'mila23081985@ya.ru'],
        ['Клима', 'Варвара', '+7 (906) 878-14-63', 'varvara06031966@hotmail.com'],
        ['Углицкий', 'Иван', '+7 (963) 428-18-95', 'ivan30@yandex.ru'],
        ['Лачков', 'Степан', '+7 (995) 495-44-13', 'stepan1962@mail.ru'],
        ['Шихина', 'Екатерина', '+7 (994) 332-24-99', 'ekaterina1992@ya.ru'],
        ['Дасаев', 'Петр', '+7 (963) 761-17-33', 'petr.dasaev@gmail.com'],
        ['Максимова', 'Арина', ['+7 (915) 714-95-53', '+7 (993) 395-58-24'], 'arina03101983@mail.ru'],
        ['Цирульников', 'Тарас', '+7 (912) 806-10-22', 'taras26121985@rambler.ru'],
        ['Хрустицкий', 'Савва', '+7 (946) 370-28-67', 'savva.hrustickiy@outlook.com'],
        ['Валеева', 'Людмила', '+7 (987) 765-99-55, +7 (993) 124-98-21, +7 (966) 712-24-43', 'lyudmila2793@rambler.ru'],
        ['Зайкова', 'София', '+7 (992) 101-93-90', 'sofiya.zaykova@mail.ru'],
        ['Эристова', 'Полина', '+7 (949) 161-46-22', 'polina24111981@hotmail.com']
    ]

    for client in clients:
        add_client(conn, client[1], client[0], client[3], client[2])

    # Поиск клиента
    client = find_client(conn, last_name='Еремеев')
    if client[0] is not None:
        client = client[0]
        client_id = client[0]
        fio = client[2] + ' ' + client[1]
        print(f'Найден: {fio} контакты: {client[3]} {client[4]}')

        # Удаляем клиенту номер телефона
        phone_number = '+7 (953) 344-23-19'
        res = delete_phone(conn, client_id, phone_number)
        if res[0]:
            print(f'Номер телефона "{phone_number}" у "{fio}" удален')
        else:
            print(f'Ошибка при удалении номера телефона ({phone_number}) у "("{fio}" {res[1]}')

        # и добавляем новый
        phone_number = '+7 (923) 309-94-35'
        res = add_phone(conn, client_id, phone_number)
        if res[0]:
            print(f'"{fio}" добавили новый номер телефона "{phone_number}"')
        else:
            print(f'Ошибка при добавлении "{fio}" номера телефона "{phone_number}" {res[1]}')

        # Теперь сменим ему фамилию
        res = change_client(conn, client_id, last_name='Еркулаев')
        if res[0]:
            print(f'Профиль client_id="{client_id}" обновлен')
        else:
            print(f'Ошибка при обновлении профиля "{client_id}" {res[1]}')

        # И вообще удаляем его из базы
        res = delete_client(conn, client_id)
        if res[0]:
            print(f'"{fio}" удален')
        else:
            print(f'Ошибка при удалении "{fio}" {res[1]}')
