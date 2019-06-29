FROM python:3

ENV JAVA_HOME=/usr/local/java
ENV SPARK_HOME=/usr/local/spark

ADD requirements.txt /
RUN pip3 install -r requirements.txt


ADD . /src
COPY luigi.cfg /etc/luigi/

RUN groupadd -g 1001 hadoop && \
    useradd -r -u 1000 -g hadoop kharivitalij
USER kharivitalij

ENTRYPOINT [ "python3", "/src/python-modules/app.py" ]