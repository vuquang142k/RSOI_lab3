import os
import requests
import pika
import json
import time
import functools
from flask import Flask, request, Response

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

baseUrlBonus = 'http://10.5.0.4:8050'
baseUrlFlight = 'http://10.5.0.5:8060'
baseUrlTickets = 'http://10.5.0.6:8070'


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


@app.route('/api/v1/flights', methods=['GET'])
@circuit_breaker(max_retries=5, service_name='Flight')
def GWS_get_flights():
    headers = {'Content-type': 'application/json'}
    param = dict(request.args)
    response = requests.get(baseUrlFlight + '/api/v1/flights', params=param, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        return Response(status=404)


@app.route('/api/v1/privilege', methods=['GET'])
@circuit_breaker(max_retries=5, service_name='Bonus')
def GWS_get_privilege():

    response = requests.get(baseUrlBonus + '/api/v1/privilege', headers=request.headers)

    if response.status_code == 200:
        return response.json()
    else:
        return Response(status=404)


@app.route('/api/v1/me', methods=['GET'])
def GWS_get_me_info():
    result = dict()
    result['tickets'] = GWS_get_tickets()

    # If the ticket service is unavailable, return the fallback
    if type(result['tickets']) is tuple:
        result['tickets'] = []

    result['privilege'] = GWS_get_privilege()

    # If the bonus service is unavailable, return the fallback
    if type(result['privilege']) is tuple:
        result['privilege'] = []
    else:
        del result['privilege']['history']
    return result


@circuit_breaker(max_retries=5, service_name='Ticket')
def get_info_tickets(api, headers):
    return requests.get(api, headers=headers)


@circuit_breaker(max_retries=5, service_name='Flight')
def get_info_flights(api, data):
    return requests.get(api, data=data).json()


@app.route('/api/v1/tickets', methods=['GET'])
def GWS_get_tickets():

    # Get ticket Uid, flight number and status
    info_tickets = get_info_tickets(baseUrlTickets + '/api/v1/tickets', headers=request.headers)

    # If the ticket service is unavailable, return error 503
    if type(info_tickets) is tuple:
        return info_tickets

    info_tickets = info_tickets.json()

    for ticket in info_tickets:
        if 'message' in info_tickets:
            continue
        info_flights = get_info_flights(baseUrlFlight + '/api/v1/flights/exist', data=ticket['flightNumber'])

        # If the flight service is unavailable, return the fallback
        if type(info_flights) is tuple:
            ticket['fromAirport'], ticket['toAirport'], ticket['date'], ticket['price'] = '', '', '', ''
            continue

        ticket['fromAirport'] = info_flights['fromAirport']
        ticket['toAirport'] = info_flights['toAirport']
        ticket['date'] = info_flights['date']
        ticket['price'] = info_flights['price']

    return info_tickets


def rollback_ticket(ticket_uid):
    return requests.delete(baseUrlTickets + f'/api/v1/tickets/rollback/{ticket_uid}')


@app.route('/api/v1/tickets', methods=['POST'])
def GWS_post_tickets():
    # Get purchase information
    buy_info = request.json

    # Get username
    username = request.headers['X-User-Name']

    # Checking the existing flight number
    flight_exist = get_info_flights(baseUrlFlight + '/api/v1/flights/exist', data=buy_info['flightNumber'])

    # If the flight service is unavailable, return error 503
    if type(flight_exist) is tuple:
        return flight_exist

    # Return Error: 404 Not Found if flight number don't exist
    if not flight_exist:
        return Response(status=404)

    # Information for the ticket database
    data = {'username': username,
            'flightNumber': flight_exist['flightNumber'],
            'price': flight_exist['price'],
            'status': 'PAID'}

    # Check ticket service is available
    info_tickets = get_info_tickets(api=baseUrlTickets + '/api/v1/tickets', headers=request.headers)

    # If the ticket service is unavailable, return error 503
    if type(info_tickets) is tuple:
        return info_tickets

    # Get ticket UID
    ticket_uid = requests.post(baseUrlTickets + '/api/v1/tickets/buy', json=data)

    # Fill the first part of the response
    response = dict()
    response['ticketUid'] = ticket_uid.text
    response['flightNumber'] = flight_exist['flightNumber']
    response['fromAirport'] = flight_exist['fromAirport']
    response['toAirport'] = flight_exist['toAirport']
    response['date'] = flight_exist['date']
    response['price'] = flight_exist['price']
    response['status'] = 'PAID'

    # Information about privileges after ticket purchase (the third part of the response)
    # privilege_info = requests.get(baseUrlBonus + '/api/v1/privilege', headers=request.headers).json()
    privilege_info = GWS_get_privilege()

    # If the bonus service is unavailable, return error 503
    if type(privilege_info) is tuple:
        rollback_status = rollback_ticket(ticket_uid.text)
        app.logger.info(rollback_status)
        return privilege_info

    del privilege_info['history']
    response['privilege'] = privilege_info

    # Processing bonus points (the second part of the response)
    if buy_info['paidFromBalance']:
        # Debiting from the bonus account
        data = {'username': username, 'ticketUid': ticket_uid.text, 'price': int(flight_exist['price'])}
        paid_by_bonuses = int(requests.post(baseUrlBonus + '/api/v1/privilege/debit', json=data).text)

        response['paidByMoney'] = data['price'] - paid_by_bonuses
        response['paidByBonuses'] = paid_by_bonuses
    else:
        # Replenishment of the bonus account
        data = {'username': username, 'ticketUid': ticket_uid.text, 'price': int(flight_exist['price'])}
        requests.post(baseUrlBonus + '/api/v1/privilege/replenishment', json=data)

        response['paidByMoney'] = flight_exist['price']
        response['paidByBonuses'] = 0

    return response


@app.route('/api/v1/tickets/<string:ticketUid>', methods=['GET'])
def GWS_get_ticket_by_uid(ticketUid):

    # Get flight number and status
    info_tickets = get_info_tickets(baseUrlTickets + f'/api/v1/tickets/{ticketUid}', headers=request.headers)

    # If the ticket service is unavailable, return error 503
    if type(info_tickets) is tuple:
        return info_tickets

    if info_tickets.status_code != 200:
        return Response(status=404)

    info_tickets = info_tickets.json()

    response = dict()
    response['ticketUid'] = ticketUid
    response['flightNumber'] = info_tickets['flightNumber']
    response['status'] = info_tickets['status']

    # Get flight number and status
    info_flights = get_info_flights(baseUrlFlight + '/api/v1/flights/exist', data=info_tickets['flightNumber'])

    # If the flight service is unavailable, return the fallback
    if type(info_flights) is tuple:
        response['fromAirport'], response['toAirport'], response['date'], response['price'] = '', '', '', ''
    else:
        response['fromAirport'] = info_flights['fromAirport']
        response['toAirport'] = info_flights['toAirport']
        response['date'] = info_flights['date']
        response['price'] = info_flights['price']

    return response


@app.route('/api/v1/tickets/<string:ticketUid>', methods=['DELETE'])
def GWS_ticket_refund(ticketUid):
    # Check ticket service is available
    info_tickets = get_info_tickets(api=baseUrlTickets + '/api/v1/tickets', headers=request.headers)

    # If the ticket service is unavailable, return error 503
    if type(info_tickets) is tuple:
        return info_tickets

    tickets_response = requests.delete(baseUrlTickets + f'/api/v1/tickets/{ticketUid}')
    if tickets_response.status_code != 204:
        return Response(status=404)

    # Checking the availability of the Bonus service
    privilege_info = GWS_get_privilege()

    # If the bonus service is unavailable, use rabbitmq
    if type(privilege_info) is tuple:
        cmd = dict()
        cmd['status'] = 'ticket_refund'
        cmd['headers'] = dict(request.headers)
        cmd['api'] = baseUrlBonus + f'/api/v1/privilege/{ticketUid}'
        cmd = json.dumps(cmd)

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='task_queue', durable=True)
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=cmd.encode(),
            properties=pika.BasicProperties(
                delivery_mode=2,
            ))
        connection.close()
    else:
        privilege_response = requests.delete(baseUrlBonus + f'/api/v1/privilege/{ticketUid}',
                                             headers=request.headers)
        if privilege_response.status_code != 204:
            return Response(status=404)

    return Response(status=204)


@app.route('/manage/health', methods=['GET'])
def GWS_manage_health():
    return Response(status=200)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(debug=True, port=8080, host="0.0.0.0")
