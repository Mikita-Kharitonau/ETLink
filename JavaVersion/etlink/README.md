# etlink (ETL service)

Java version of ETLink application.

## How to run?

1. **Prepare database**. You can run it on your host or in docker container with the following command:
    ```
    $ docker run -e MYSQL_ROOT_PASSWORD=12345678 -e MYSQL_USER=etlink -e MYSQL_PASSWORD=etlink -e MYSQL_DATABASE=etlink_java -p 3306:3306 --network host mysql
    ```
    Create table for counted links:
    ```
    CREATE TABLE etlink_java.counted_links ( 
        link varchar(500),
        count int
    );
    ```
2. **Run application**
    ```
    $ ./gradlew run
    ```
3. **Make sure** you have database populated:
    ```
    select count(*) from etlink_java.counted_links;
    11195
    ```

    ```
    select * from etlink_java.counted_links order by count desc;
    
    link,count
    https://en.wikipedia.org/wiki/International_Standard_Book_Number,228
    https://en.wikipedia.org/wiki/Napoleon#CITEREFMcLynn1998,64
    https://en.wikipedia.org/wiki/League_of_Nations#CITEREFNorthedge1986,53
    https://en.wikipedia.org/wiki/Digital_object_identifier,32
    https://en.wikipedia.org/wiki/British_Empire#refLloyd1996,30
    https://en.wikipedia.org/wiki/League_of_Nations#CITEREFScott1973,24
    https://en.wikipedia.org/wiki/British_Empire#refJames2001,23
    https://en.wikipedia.org/wiki/Ottoman_Empire,21
    https://en.wikipedia.org/wiki/Austrian_Empire,20
    https://en.wikipedia.org/wiki/Russian_Empire,20
    ```