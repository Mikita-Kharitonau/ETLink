FROM python:3

ENV JAVA_HOME=/home/kharivitalij/Software/jdk1.8.0_211
ENV SPARK_HOME=/home/kharivitalij/Software/spark-2.4.3-bin-hadoop2.7
ENV PYSPARK_PYTHON=python3

ADD requirements.txt /
RUN pip3 install -r requirements.txt


ADD . /src
COPY luigi.cfg /etc/luigi/

RUN groupadd -g 1001 hadoop && \
    useradd -r -u 1000 -g hadoop kharivitalij
USER kharivitalij

ENTRYPOINT [ "python3", "/src/python-modules/app.py" ]