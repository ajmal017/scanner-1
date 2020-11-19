FROM python:3.7

ADD . /src
COPY requirements.txt /src
RUN pip install -r /src/requirements.txt

WORKDIR /src

EXPOSE 5000

CMD [ "python", "-u", "./src/levitrade.v2.scanner.py" ]