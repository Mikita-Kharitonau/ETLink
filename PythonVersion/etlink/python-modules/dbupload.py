from luigi.contrib.spark import PySparkTask
from pyspark.sql import SQLContext
import luigi.contrib.hdfs

from saving import Saving
from config import (
    DB_NAME,
    DB_TABLE,
    DB_USER,
    DB_PASSWORD,
    MYSQL_HOST,
    MYSQL_PORT
)

class DBUpload(PySparkTask):
    """
    DBUpload task.
    Get dataframe back from Saving task and store it in mysql.
    Depends on Saving task.
    """

    urls = luigi.ListParameter()

    def requires(self):
         return Saving(urls=self.urls)

    def main(self, sc, *args):
        sqlContext = SQLContext(sc)
        df = sqlContext.read.format('parquet').load(self.input().path)
        df.show()

        df.write.format('jdbc').options(
            url='jdbc:mysql://{host}:{port}/{db_name}'.format(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                db_name=DB_NAME
            ),
            driver='com.mysql.jdbc.Driver',
            dbtable=DB_TABLE,
            user=DB_USER,
            password=DB_PASSWORD
        ).mode('append').save()



