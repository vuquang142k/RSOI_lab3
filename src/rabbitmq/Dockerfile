From python:3.11

COPY . /rabbitmq
COPY requirements.txt /rabbitmq
WORKDIR /rabbitmq
RUN pip install -r requirements.txt

EXPOSE 8040
CMD gunicorn --workers=4 --bind 0.0.0.0:8040 app:app