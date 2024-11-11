import os
import requests
import pika
import json
import time
import functools

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)


# ports
# gateway 8080
# flights 8060
# privilege 8050
# ticket 8070

flights_ip = "flight"
privilege_ip = "privilege"
ticket_ip = "ticket"

def circuit_breaker(max_retries, service_name):
    def circuit_breaker_decorator(func):
        @functools.wraps(func)
        def _wrapper(*args, **kwargs):
            for _ in range(max_retries):
                try:
                    result = func(*args, **kwargs)
                    return result
                except:
                    time.sleep(1)
            return {'message': f'{service_name} Service unavailable'}, 503
        return _wrapper
    return circuit_breaker_decorator

@app.route('/')
def GWS_hello_world():
    statement = 'Gateway service!'
    return statement

@app.route('/manage/health', methods=['GET'])
def GWS_health_check():
    return 200

# Получить список всех перелетов
@app.route('/api/v1/flights', methods=["GET"])
@circuit_breaker(max_retries=5, service_name='Flight')
def get_flights():
    print(request.args)
    page = request.args.get("page")
    size = request.args.get("size")
    print(page)
    print(size)
    flight_response = requests.get(url=f"http://{flights_ip}:8060/api/v1/flights?page={page}&size={size}")
    # if flight_response.status_code == 202:
    #     return {}, 202
    if flight_response.status_code == 200:
        print(123)
        return flight_response.json(), 200
    else:
        print(44)
        return "не найдены полеты", 404

@app.route('/api/v1/privilege', methods=['GET'])
@circuit_breaker(max_retries=5, service_name='Bonus')
def GWS_get_privilege():

    response = requests.get(url=f"http://{privilege_ip}:8050/api/v1/privilege", headers=request.headers)

    if response.status_code == 200:
        return response.json(), 200
    else:
        return "не найдены билеты пользователя", 404

# Получить полную информацию о пользователе
# Возвращается информация о билетах и статусе в системе привилегии.
# X-User-Name: {{username}}
@app.route('/api/v1/me', methods=["GET"])
def get_person():
    user = request.headers
    user = user["X-User-Name"]
    tickets_info = requests.get(url=f"http://{ticket_ip}:8070/api/v1/tickets/{user}")
    privilege_info = requests.get(url=f"http://{privilege_ip}:8050/api/v1/privilege/{user}")

    if tickets_info.status_code != 200 and privilege_info.status_code != 200:
        return "не найдена программа привилегий для пользователя и билеты пользователя", 404

    if tickets_info.status_code != 200:
        return "не найдены билеты пользователя", 404

    if privilege_info.status_code != 200:
        return "не найдена программа привилегий для пользователя", 404

    user_info = {
        "tickets": tickets_info.json(),
        "privilege": privilege_info.json()
    }
    return user_info, 200

@circuit_breaker(max_retries=5, service_name='Ticket')
def get_info_tickets(api, headers):
    return requests.get(api, headers=headers)


@circuit_breaker(max_retries=5, service_name='Flight')
def get_info_flights(api, data):
    return requests.get(api, data=data).json()

# Получить информацию о всех билетах пользователя
# X-User-Name: {{username}}
@app.route('/api/v1/tickets', methods=["GET"])
def get_tickets():
    user = request.headers
    user = user["X-User-Name"]

    tickets_info = requests.get(url=f"http://{ticket_ip}:8070/api/v1/tickets/{user}")
    if tickets_info.status_code == 200:
        return tickets_info.json(), 200
    else:
        return "не найдены билеты пользователя", 404


# Получить информацию о всех билетах пользователя
# X-User-Name: {{username}}
@app.route('/api/v1/tickets/<ticketUid>', methods=["GET"])
def get_ticket(ticketUid: str):
    user = request.headers
    user = user["X-User-Name"]
    ticket_info = requests.get(url=f"http://{ticket_ip}:8070/api/v1/tickets/{user}/{ticketUid}")
    if ticket_info.status_code == 200:
        return ticket_info.json(), 200
    else:
        return "не найдены билеты пользователя", 404


# Возврат билета
# X-User-Name: {{username}}
@app.route('/api/v1/tickets/<ticketUid>', methods=["DELETE"])
def delete_ticket(ticketUid: str):
    user = request.headers
    user = user["X-User-Name"]
    ticket_info = requests.delete(url=f"http://{ticket_ip}:8070/api/v1/tickets/{user}/{ticketUid}")
    if ticket_info.status_code != 200:
        return "Не найден билет", 404
    json_uid = {
        "ticketUid": ticketUid
    }
    status = requests.post(url=f"http://{privilege_ip}:8050/api/v1/back_bonuses", json=json_uid,
                           headers={"X-User-Name": user})
    if status.status_code != 200:
        return "Не найдена программа боунусов, билет возвращен", 404
    return "Билет успешно возвращен", 204


# Покупка билета
# POST {{baseUrl}}/api/v1/tickets
# Content-Type: application/json
# X-User-Name: {{username}}
#
# {
#   "flightNumber": "AFL031",
#   "price": 1500,
#   "paidFromBalance": true
# }
@app.route('/api/v1/tickets', methods=["POST"])
def post_ticket():
    # проверка существования рейса (flightNumber), если флаг привелегий установлен то списываем привелегии
    # если нет то добавляем 10 процентов от стоимости билета
    user = request.headers
    user = user["X-User-Name"]
    json_req = request.json

    flight_info = requests.get(url=f'http://{flights_ip}:8060/api/v1/flights/{json_req["flightNumber"]}')
    if flight_info.status_code != 200:
        return "не найден рейс", 404
    json_flight = flight_info.json()

    ticket_info = requests.post(url=f"http://{ticket_ip}:8070/api/v1/tickets", json=json_req,
                                headers={"X-User-Name": user})
    json_ticket = ticket_info.json()

    priv_json_send = {
        "paidFromBalance": json_req["paidFromBalance"],
        "ticketUid": json_ticket["ticketUid"],
        "price": json_req["price"]
    }

    privil_info = requests.post(url=f"http://{privilege_ip}:8050/api/v1/buy", json=priv_json_send,
                                headers={"X-User-Name": user})
    json_privil = privil_info.json()

    json_out = {
        "ticketUid": json_ticket["ticketUid"],
        "flightNumber": json_req["flightNumber"],
        "fromAirport": json_flight["fromAirport"],
        "toAirport": json_flight["toAirport"],
        "date": json_flight["date"],
        "price": json_req["price"],
        "paidByBonuses": json_privil["paidByBonuses"],
        "paidByMoney": json_privil["paidByMoney"],
        "status": json_ticket["status"],
        "privilege": {
            "balance": json_privil["balance"],
            "status": json_privil["status"]
        }
    }
    print(json_req["price"])
    print(json_privil["paidByMoney"])

    return json_out, 200

    # return app.redirect(location=f'{request.host_url}api/v1/persons/{int(person_id)}', code=201)


# Получить информацию о состоянии бонусного счета
# X-User-Name: {{username}}
@app.route('/api/v1/privilege', methods=["GET"])
def get_privilege():
    user = request.headers
    user = user["X-User-Name"]
    privilege_info = requests.get(url=f"http://{privilege_ip}:8050/api/v1/privileges/{user}")
    return privilege_info.json(), 200


@app.route(f"/api/v1/flights/<flight_number>", methods=["GET"])
def get_flight_byticket(flight_number: str):
    req = requests.get(f"http://{flights_ip}:8060/api/v1/flights/{flight_number}")
    return req.json(), 200


if __name__ == '__main__':
    app.run(port=8080, debug=True)
