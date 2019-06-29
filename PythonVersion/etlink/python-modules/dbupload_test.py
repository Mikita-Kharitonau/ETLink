import unittest
import subprocess

from pyspark.context import SparkContext
from pyspark.sql import (
    Row,
    SparkSession,
    SQLContext
)

from saving import Saving
from dbupload import DBUpload
from config import (
    APPLICATION_NAME,
    EXTRACTION_OUTPUT,
    DB_NAME,
    DB_TABLE,
    DB_USER,
    DB_PASSWORD,
    MYSQL_HOST,
    MYSQL_PORT
)


TEST_URL_1 = "https://en.wikipedia.org/wiki/Napoleon.html"
TEST_URL_2 = "https://en.wikipedia.org/wiki/Battle_of_Austerlitz.html"

class TestDBUpload(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dbupload_task = DBUpload([TEST_URL_1, TEST_URL_2])
        cls.saving_task = Saving([TEST_URL_1, TEST_URL_2])
        cls.dbupload_task.input = lambda : cls.saving_task.output()

        cls.cmd("hadoop fs -rm -r /{app_name}".format(app_name=APPLICATION_NAME).split())

        cls.sc = SparkContext()
        spark = SparkSession(cls.sc)
        cls.expected_dataframe = spark.createDataFrame([
            Row(link="https://en.wikipedia.org/wiki/Napoleon#p-search", count=2),
            Row(link="https://en.wikipedia.org/wiki/Napoleon_Bonaparte_(disambiguation)", count=4),
            Row(link="https://en.wikipedia.org/wiki/Notre-Dame_Cathedral", count=1),
            Row(link="https://en.wikipedia.org/w/index.php?title=Battle_of_Austerlitz&oldid=855857008", count=1),
            Row(link="https://www.imdb.com/title/tt0053638/", count=1),
            Row(link="https://en.wikisource.org/wiki/The_New_International_Encyclop%C3%A6dia/Austerlitz", count=1),
            Row(link="https://commons.wikimedia.org/wiki/Category:Battle_of_Austerlitz", count=1),
            Row(link="https://en.wikipedia.org/wiki/Napoleon_(disambiguation)", count=3),
            Row(link="https://en.wikipedia.org/wiki/Coronation_of_Napoleon_I", count=1),
            Row(link="https://en.wikipedia.org/wiki/Napoleon_Bonaparte%27s_battle_record", count=1),
            Row(link="https://books.google.com/books?id=MZoO7SIwMVIC&pg=PA133", count=1),
            Row(link="https://en.wikipedia.org/wiki/Napoleon_III", count=3),
            Row(link="https://en.wikipedia.org/wiki/Emperor_of_the_French", count=1),
            Row(link="https://en.wikipedia.org/wiki/Battle_of_Austerlitz", count=1),
            Row(link="https://en.wikipedia.org/wiki/Napoleon#mw-head", count=2)
        ])
        cls.expected_dataframe.write.format("parquet").save(cls.saving_task.output().path)

    @classmethod
    def tearDownClass(cls):
        cls.cmd("hadoop fs -rm -r /{app_name}".format(app_name=APPLICATION_NAME).split())
        cls.sc.stop()

    @classmethod
    def cmd(cls, args):
        print("Test command: {cmd}".format(cmd=" ".join(args)))
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate()

        return proc.returncode

    def test_main(self):
        spark = SparkSession(self.sc)
        sqlContext = SQLContext(self.sc)

        self.dbupload_task.main(self.sc)

        writed_df = sqlContext.read.format('jdbc').options(
            url='jdbc:mysql://{host}:{port}/{db_name}'.format(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                db_name=DB_NAME
            ),
            driver='com.mysql.jdbc.Driver',
            dbtable=DB_TABLE,
            user=DB_USER,
            password=DB_PASSWORD
        ).load()

        writed_df.show()

        self.assertEqual(self.expected_dataframe.select("link", "count").subtract(writed_df).count(), 0)

