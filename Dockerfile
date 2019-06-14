FROM python:3.7.3
WORKDIR /app
CMD ["python", "/app"]

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

ADD . /app
