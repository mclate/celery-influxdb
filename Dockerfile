FROM python:3-alpine
WORKDIR /app
CMD ["python", "/app"]

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

ADD . /app
