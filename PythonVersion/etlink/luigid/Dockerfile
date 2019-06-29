FROM python:3

ADD luigid.py /
COPY requirements.txt /

RUN pip3 install -r requirements.txt

EXPOSE 8082

CMD ["python3", "luigid.py"]
