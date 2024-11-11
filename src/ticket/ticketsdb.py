import psycopg2

#DB_URL = "host='localhost' port = '5432' dbname='microservice-flight' user='postgres' password='quang' "
#DB_URL = "host='postgres' port = '5432' database='tickets' user='program' password='test'"
DB_URL = "postgresql://program:test@postgres:5432/tickets"
# password = "test"
# user = "program"
# dbname = "postgres"
# port = "5432"
# host = "postgres"
# database = "flight"


def create_ticketsdb():
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ticket
                    (
                    id            SERIAL PRIMARY KEY,
                    ticket_uid    uuid UNIQUE NOT NULL,
                    username      VARCHAR(80) NOT NULL,
                    flight_number VARCHAR(20) NOT NULL,
                    price         INT         NOT NULL,
                    status        VARCHAR(20) NOT NULL
                        CHECK (status IN ('PAID', 'CANCELED'))
                    );
                   """)
    db.commit()
    cursor.close()
    db.close()
    return


def get_user_flight(user: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"""SELECT ticket_uid, flight_number, price, status
                       FROM ticket 
                       WHERE ticket.username = '{user}';""")
    flight = cursor.fetchall()
    cursor.close()
    db.close()
    return flight


def get_one_flight(ticketUid: str, user: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"""SELECT ticket_uid, flight_number, price, status 
                       FROM ticket  
                       WHERE ticket_uid = '{ticketUid}' and username = '{user}';""")
    flight = cursor.fetchone()
    cursor.close()
    db.close()
    return flight


def add_ticker(ticketUid: str, user: str, flight_number: str, price: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"INSERT INTO ticket (id, ticket_uid, username, flight_number, price, status) "
                   f"VALUES (DEFAULT, '{ticketUid}', '{user}', '{flight_number}', {price}, 'PAID');")
    db.commit()
    cursor.close()
    db.close()
    return True


def change_ticker_status(ticketUid: str, user: str):
    db = psycopg2.connect(DB_URL)
    cursor = db.cursor()
    cursor.execute(f"""UPDATE ticket SET status = 'CANCELED' WHERE ticket_uid = '{ticketUid}' and username = '{user}'""")
    db.commit()
    cursor.close()
    db.close()
    return True
