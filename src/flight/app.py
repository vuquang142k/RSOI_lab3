import flightdb
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)

@app.route('/')
def FS_hello_world():
    statement = 'Flight service!'
    return statement

@app.route('/manage/health', methods=['GET'])
def FS_health_check():
    return 200

@app.route('/api/v1/flights', methods=["GET"])
def get_flights():
    flightdb.create_flightsdb()
    page = int(request.args.get("page"))
    size = int(request.args.get("size"))
    flights = flightdb.get_flights(page, size)
    totalElements = len(flights)
    jsflights = []
    print(flights)
    for flight in flights:
        jsfligt = {
            "flightNumber": flight[0],
            "fromAirport": flight[3] + " " + flight[4],
            "toAirport": flight[1] + " " + flight[2],
            "date": flight[5],
            "price": flight[6],
        }
        jsflights.append(jsfligt)

    json_flights = {
        "page": page,
        "pageSize": size,
        "totalElements": totalElements,
        "items": jsflights
    }
    print(json_flights)
    return json_flights, 200

@app.route('/api/v1/flights/<flight_num>', methods=["GET"])
def get_flights_byticket(flight_num: str):
    flightdb.create_flightsdb()
    info_flight = flightdb.get_flights_bynum(flight_num)
    if info_flight:
        json_flight = {
            "flightNumber": info_flight[0],
            "fromAirport": info_flight[3] + " " + info_flight[4],
            "toAirport": info_flight[1] + " " + info_flight[2],
            "date": info_flight[5],
            "price": info_flight[6],
        }

        return json_flight, 200

    else:
        print("error")
        return 404


if __name__ == '__main__':
    flightdb.create_flightsdb()
    app.run(port=8060)

