import uuid
import ticketsdb
import requests

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)


gateway_ip = "gateway"

@app.route('/')
def TS_hello_world():
    statement = 'Ticket service!'
    return statement

@app.route('/manage/health', methods=['GET'])
def TS_health_check():
    return 200

@app.route('/api/v1/tickets/<user>', methods=["GET"])
def get_tickets(user: str):
    ticketsdb.create_ticketsdb()
    # получить полеты
    js_tickets = []
    tickets = ticketsdb.get_user_flight(user)
    if tickets:
        for ticket in tickets:
            print()
            req = requests.get(url=f"http://{gateway_ip}:8080/api/v1/flights/{ticket[1]}")
            info_flights = req.json()
            js_ticket = {
                "ticketUid": ticket[0],
                "flightNumber": ticket[1],
                "fromAirport": info_flights["fromAirport"],
                "toAirport": info_flights["toAirport"],
                "date": info_flights["date"],
                "price": ticket[2],
                "status": ticket[3]
            }
            js_tickets.append(js_ticket)

        return js_tickets, 200

    else:
        return {}, 404


@app.route('/api/v1/tickets', methods=["POST"])
def get_tickets_post():
    ticketsdb.create_ticketsdb()
    user = request.headers
    user = user["X-User-Name"]
    json = request.json
    ticketUid = str(uuid.uuid4())
    print("генератор юайди")
    print(ticketUid)
    isadded = ticketsdb.add_ticker(ticketUid, user, json["flightNumber"], json["price"])
    json_ticket = {
        "ticketUid": ticketUid,
        "status": "PAID"
    }

    if isadded:
        return json_ticket, 200
    else:
        return 400


@app.route('/api/v1/tickets/<user_login>/<ticketUid>', methods=["GET"])
def get_oneticket(user_login: str, ticketUid: str):
    ticketsdb.create_ticketsdb()
    # получить полет
    ticket = ticketsdb.get_one_flight(ticketUid, user_login)
    req = requests.get(url=f"http://{gateway_ip}:8080/api/v1/flights/{ticket[1]}")
    info_flights = req.json()
    js_ticket = {
        "ticketUid": ticketUid,
        "flightNumber": ticket[1],
        "fromAirport": info_flights["fromAirport"],
        "toAirport": info_flights["toAirport"],
        "date": info_flights["date"],
        "price": ticket[2],
        "status": ticket[3]
    }
    return js_ticket, 200

@app.route('/api/v1/tickets/<user_login>/<ticketUid>', methods=["DELETE"])
def delete_ticket(user_login: str, ticketUid: str):
    ticketsdb.create_ticketsdb()
    status = ticketsdb.change_ticker_status(ticketUid, user_login)
    print(status)
    if status:
        return {}, 200
    else:
        return {}, 404


if __name__ == '__main__':
    ticketsdb.create_ticketsdb()
    app.run(port=8070)
