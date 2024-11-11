import pika
import time
import json
import requests


def worker_ticket_refund(headers, api):
    status_code = 500
    while status_code != 204:
        try:
            privilege_response = requests.delete(api, headers=headers)
            status_code = privilege_response.status_code
        except:
            time.sleep(10)


def callback(ch, method, properties, body):
    print(" [x] Received %s" % body)
    cmd = json.loads(body.decode())

    if cmd['status'] == 'ticket_refund':
        worker_ticket_refund(cmd['headers'], cmd['api'])
    else:
        print("sorry i did not understand ", body)

    print(" [x] Done")

    ch.basic_ack(delivery_tag=method.delivery_tag)


sleepTime = 10
print(' [*] Sleeping for ', sleepTime, ' seconds.')
time.sleep(sleepTime)

print(' [*] Connecting to server ...')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

print(' [*] Waiting for messages.')

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)
channel.start_consuming()
