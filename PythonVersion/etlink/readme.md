# etlink (ETL service)

- files stored in hdfs
- **R**esilient **D**istributed **D**atasets and Dataframes are used to manage data and save it to db.
 
#### Install and setup:

* [Docker](https://docs.docker.com/install/)

* [Docker compose](https://docs.docker.com/compose/install/)

* [Hadoop](http://hadoop.apache.org/) 

* [PySpark](https://pypi.org/project/pyspark/) 


Before you start, make sure that hdfs running 

 ## How to test?
 
1. Go to the directory with python modules.
```
$ cd etlink/python-modules
```
2. Run tests.
```
$ python3 -m unittest discover -s . -p "*_test.py"
```
 
 ## How to run?

1. Go to the etlink directory.
```
$ cd etlink
```
2. Build docker image for application.
```
$ docker build -t workflow .
```
3. Run luigi, mysql, adminer services using docker-compose.
```
$ docker-compose up
```
4. Run application image in container.
```
$ docker run -v $HADOOP_HOME:$HADOOP_HOME -v $JAVA_HOME:JAVA_HOME -v $SPARK_HOME:$SPARK_HOME --network host workflow url1 url2 url3
```

Please, feel free to ask your questions - nikita.kharitonov99@gmail.com

