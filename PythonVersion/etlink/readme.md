# etlink (ETL service)

Python version of ETLink application.

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
3. Run luigi, mysql services using docker-compose.
```
$ docker-compose up
```
4. Run application image in container.
```
$ docker run -v $HOME:$HOME --network host workflow url1 url2 url3
```

## Simple workflow example

#### Make sure, database isn't populated yet:

Connect to mysql instance running on localhost:3306 with credentials:
```
MYSQL_DATABASE: test
MYSQL_USER: workflow
MYSQL_PASSWORD: exercise
```
And run
```
select count(*) from data order by count desc;
```

![Empty database](./readme_images/Screenshot17.png)

#### We will run next command:

```
$ docker run -v $HOME:$HOME --network host workflow https://en.wikipedia.org/wiki/Napoleon https://ru.wikipedia.org/wiki/Audi https://ru.wikipedia.org/wiki/BMW https://ru.wikipedia.org/wiki/Volkswagen https://ru.wikipedia.org/wiki/Dodge https://ru.wikipedia.org/wiki/Fiat https://ru.wikipedia.org/wiki/Ford
```
to retrieve data from listed sources.

#### Navigate to [Luigi scheduler Web UI](localhost:8082) to see tasks states and dependency graph:

![](./readme_images/Screenshot2.png)
![](./readme_images/Screenshot3.png)
![](./readme_images/Screenshot4.png)
![](./readme_images/Screenshot5.png)
![](./readme_images/Screenshot8.png)
![](./readme_images/Screenshot9.png)

#### Navigate to [http://localhost:8088](localhost:8088) to see hadoop applications on yarn scheduler:

![](./readme_images/Screenshot6.png)
![](./readme_images/Screenshot7.png)
![](./readme_images/Screenshot10.png)

#### Navigate to [http://localhost:9870](localhost:9870) to see HDFS related information:

![](./readme_images/Screenshot11.png)
![](./readme_images/Screenshot12.png)
Extraction tasks output:
![](./readme_images/Screenshot13.png)
Saving task output:
![](./readme_images/Screenshot14.png)

#### And finally check mysql database for results:

```
select * from data order by count desc;
```
![](./readme_images/Screenshot15.png)

```
select count(*) from data order by count desc;
```
![](./readme_images/Screenshot16.png)

#### You will see the following lines in the logs:
```
===== Luigi Execution Summary =====

Scheduled 9 tasks of which:
* 9 ran successfully:
    - 1 DBUpload(...)
    - 7 Extraction(url=https://en.wikipedia.org/wiki/Napoleon) ...
    - 1 Saving(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====
```

Please, feel free to ask your questions - nikita.kharitonov99@gmail.com

