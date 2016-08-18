FROM python:2.7
WORKDIR /app
CMD ["python", "/app"]

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

ADD . /app
