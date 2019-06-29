import unittest
import subprocess

from pyspark.context import SparkContext
from pyspark.sql import (
    Row,
    SparkSession,
    SQLContext
)

from saving import Saving
from extraction import Extraction
from config import (
    APPLICATION_NAME,
    EXTRACTION_OUTPUT
)

TEST_URL_1 = "https://en.wikipedia.org/wiki/Napoleon.html"
TEST_URL_2 = "https://en.wikipedia.org/wiki/Battle_of_Austerlitz.html"

class TestSaving(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.extraction_task_1 = Extraction(url=TEST_URL_1)
        cls.extraction_task_2 = Extraction(url=TEST_URL_2)

        cls.saving_task = Saving([TEST_URL_1, TEST_URL_2])
        cls.saving_task.input = lambda : [
            cls.extraction_task_1.output(),
            cls.extraction_task_2.output()
        ]
        cls.cmd("hadoop fs -rm -r /{app_name}".format(
            app_name=APPLICATION_NAME
        ).split())
        cls.cmd("hadoop fs -mkdir -p /{app_name}/{ex_out}".format(
            app_name=APPLICATION_NAME,
            ex_out=EXTRACTION_OUTPUT
        ).split())
        cls.cmd("hadoop fs -put test-resources/https--en.wikipedia.org-wiki-Battle_of_Austerlitz.html /{app_name}/{ex_out}".format(
            app_name=APPLICATION_NAME,
            ex_out=EXTRACTION_OUTPUT
        ).split())
        cls.cmd(
            "hadoop fs -put test-resources/https--en.wikipedia.org-wiki-Napoleon.html /{app_name}/{ex_out}".format(
                app_name=APPLICATION_NAME,
                ex_out=EXTRACTION_OUTPUT
        ).split())

    @classmethod
    def tearDownClass(cls):
        cls.cmd("hadoop fs -rm -r /{app_name}".format(app_name=APPLICATION_NAME).split())

    @classmethod
    def cmd(cls, args):
        print("Test command: {cmd}".format(cmd=" ".join(args)))
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate()

        return proc.returncode

    def test_main(self):
        sc = SparkContext()
        spark = SparkSession(sc)

        args = ["hadoop", "fs", "-test", "-e", self.saving_task.output().path]

        self.assertTrue(self.cmd(args))

        self.saving_task.main(sc)

        self.assertFalse(self.cmd(args))

        expected_dataframe = spark.createDataFrame([
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

        sqlContext = SQLContext(sc)
        actual_dataframe = sqlContext.read.format('parquet').load(self.saving_task.output().path)

        expected_dataframe.select("link", "count").show(20, False)
        actual_dataframe.show(20, False)

        self.assertEqual(expected_dataframe.select("link", "count").subtract(actual_dataframe).count(), 0)

