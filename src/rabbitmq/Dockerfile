FROM python:3.9
COPY . /rabbitmq
COPY requirements.txt /rabbitmq
WORKDIR /rabbitmq
RUN pip install -r requirements.txt
CMD ["python3", "worker.py"]