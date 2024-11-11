import psycopg2

#DB_URL = "host='localhost' port = '5432' dbname='microservice-flight' user='postgres' password='quang' "
#DB_URL = "host='postgres' port = '5432' database='privileges' user='program' password='test'"
DB_URL = "postgresql://program:test@postgres:5432/privileges"
# password = "test"
# user = "program"
# dbname = "postgres"
# port = "5432"
# host = "postgres"
# database = "flight"


def create_privilegedb():
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS privilege
                    (
                        id       SERIAL PRIMARY KEY,
                        username VARCHAR(80) NOT NULL UNIQUE,
                        status   VARCHAR(80) NOT NULL DEFAULT 'BRONZE'
                        CHECK (status IN ('BRONZE', 'SILVER', 'GOLD')),
                        balance  INT
                    );
                   """)
    db.commit()
    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS privilege_history
                    (
                        id             SERIAL PRIMARY KEY,
                        privilege_id   INT REFERENCES privilege (id),
                        ticket_uid     uuid        NOT NULL,
                        datetime       TIMESTAMP   NOT NULL,
                        balance_diff   INT         NOT NULL,
                        operation_type VARCHAR(20) NOT NULL
                        CHECK (operation_type IN ('FILL_IN_BALANCE', 'DEBIT_THE_ACCOUNT'))
                    );
                   """)
    db.commit()

    cursor.execute(f"SELECT privilege FROM privilege WHERE username = 'Test Max'")
    a = cursor.fetchone()
    if not a:
        cursor.execute(f"INSERT INTO privilege (id, username, status, balance) "
                       f"VALUES (DEFAULT, 'Test Max', DEFAULT, 0);")
        db.commit()
    cursor.close()
    db.close()
    return


def get_base_privilege(user: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"SELECT status, balance, id "
                   f"FROM privilege "
                   f"Where username = '{user}'")
    privilege = cursor.fetchone()
    db.commit()
    cursor.close()
    db.close()
    return privilege


def get_all_privilege(user: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"SELECT status, balance, id "
                   f"FROM privilege "
                   f"Where username = '{user}'")
    privilege = cursor.fetchone()
    cursor.execute(f"SELECT datetime, ticket_uid, balance_diff, operation_type FROM privilege_history "
                   f"WHERE privilege_id = '{privilege[2]}';")
    history = cursor.fetchall()
    db.commit()
    cursor.close()
    db.close()
    return privilege, history


def minus_bonuses(req_pay: int, user: str, ticket_uid: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"SELECT username FROM privilege WHERE username = '{user}';")
    username = cursor.fetchone()
    if not username:
        cursor.execute(f"INSERT INTO privilege (id, username, status, balance) "
                       f"VALUES (DEFAULT, '{user}', DEFAULT, 0);")
        db.commit()

    cursor.execute(f"SELECT balance, status, id FROM privilege WHERE username = '{user}';")
    status = cursor.fetchone()
    if req_pay >= status[0]:
        paid_money = req_pay - status[0]
        paid_bonus = status[0]
    else:
        paid_money = 0
        paid_bonus = req_pay

    bonuses_now = status[0] - paid_bonus
    cursor.execute(f"UPDATE privilege SET balance = {bonuses_now} WHERE username = '{user}';")
    db.commit()
    cursor.execute(
        f"INSERT INTO privilege_history (id, privilege_id, ticket_uid, datetime, balance_diff, operation_type) "
        f"VALUES (DEFAULT, '{status[2]}', '{ticket_uid}', LOCALTIMESTAMP, {paid_bonus}, 'DEBIT_THE_ACCOUNT')")
    db.commit()
    cursor.close()
    db.close()
    return [paid_money, paid_bonus, bonuses_now, status[1]]


def back_bonuses(user: str, ticket_uid: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"SELECT id, balance FROM privilege WHERE username = '{user}';")
    status = cursor.fetchone()
    old_balance = status[1]
    idd = status[0]
    cursor.execute(
        f"SELECT balance_diff, operation_type FROM privilege_history WHERE privilege_id = '{idd}' and ticket_uid = '{ticket_uid}';")
    status = cursor.fetchone()
    balance_diff = status[0]

    if status[1] == "DEBIT_THE_ACCOUNT":
        cursor.execute(
            f"INSERT INTO privilege_history (id, privilege_id, ticket_uid, datetime, balance_diff, operation_type)"
            f"VALUES (DEFAULT, '{idd}', '{ticket_uid}', LOCALTIMESTAMP, {balance_diff}, 'FILL_IN_BALANCE';")
        db.commit()
        new_balance = old_balance + balance_diff

    else:
        cursor.execute(
            f"INSERT INTO privilege_history (id, privilege_id, ticket_uid, datetime, balance_diff, operation_type)"
            f"VALUES (DEFAULT, '{idd}', '{ticket_uid}', LOCALTIMESTAMP, {balance_diff}, 'FILL_IN_BALANCE');")
        db.commit()
        new_balance = old_balance - balance_diff

    cursor.execute(f"UPDATE privilege SET balance = {new_balance} WHERE username = '{user}';")
    db.commit()
    cursor.close()
    db.close()
    return True


def add_percent(added_bonuses: int, user: str, ticket: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"SELECT username FROM privilege WHERE username = '{user}';")
    username = cursor.fetchone()
    if not username:
        cursor.execute(f"INSERT INTO privilege (id, username, status, balance) "
                       f"VALUES (DEFAULT, '{user}', DEFAULT, 0);")
        db.commit()

    cursor.execute(f"SELECT balance, status, id FROM privilege WHERE username = '{user}';")
    status = cursor.fetchone()
    new_balance = status[0] + added_bonuses

    cursor.execute(f"UPDATE privilege SET balance = {new_balance} WHERE username = '{user}';")
    db.commit()

    cursor.execute(
        f"INSERT INTO privilege_history (id, privilege_id, ticket_uid, datetime, balance_diff, operation_type)"
        f"VALUES (DEFAULT, '{status[2]}', '{ticket}', LOCALTIMESTAMP, {added_bonuses}, 'FILL_IN_BALANCE');")

    db.commit()
    cursor.close()
    db.close()
    return [new_balance, status[1]]
