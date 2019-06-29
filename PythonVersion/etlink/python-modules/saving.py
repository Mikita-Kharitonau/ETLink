import luigi
import functools

from luigi.contrib.spark import PySparkTask
import luigi.contrib.hdfs

from pyspark.sql import SparkSession
from extraction import Extraction

from config import (
    APPLICATION_NAME,
    SAVING_OUTPUT_FOLDER,
    SAVING_OUTPUT_FILE,
    HDFS_DEFAULT_NAME
)

class Saving(PySparkTask):
    """
    Saving task.
    Unites all data stored by Extraction tasks and convert resulting
    rdd to a dataframe with "link" and "count" colomns.
    Depends on Extraction tasks.
    """

    urls = luigi.ListParameter()

    def requires(self):
        """
        Depends on list of Extraction tasks.
        """

        for url in self.urls:
            yield Extraction(url)

    def get_output_name(self):
        """
        Creates a path to save output file .
        """

        return "{hdfs}/{app_name}/{folder}/{file}".format(
            hdfs=HDFS_DEFAULT_NAME,
            app_name=APPLICATION_NAME,
            folder=SAVING_OUTPUT_FOLDER,
            file=SAVING_OUTPUT_FILE
        )

    def output(self):
        """
        Returns hdfs target for output file.
        """

        return luigi.contrib.hdfs.HdfsTarget(self.get_output_name())

    def main(self, sc, *args):
        """
        Unites all data stored by Extraction tasks and convert resulting
        rdd to a dataframe with "link" and "count" colomns.
        """

        SparkSession(sc)

        if len(self.input()) > 1:
            # next line get rdd from each extraction file and unite them.
            overall_rdd = functools.reduce(lambda x, y: self.from_input_to_rdd(sc, x).
                                           union(self.from_input_to_rdd(sc, y)), self.input())
        else:
            overall_rdd = self.from_input_to_rdd(sc, self.input()[0])

        # convert to dataframe
        df = overall_rdd.map(lambda x: (x, 1)).\
                         reduceByKey(lambda x, y: x + y).\
                         toDF(["link", "count"])
        df.show()

        df.write.save(self.get_output_name(), format='parquet', mode='append')

    def from_input_to_rdd(self, sc, in_file):
        """
        Creates rdd from input hdfs file.
        """

        return sc.textFile(in_file.path)
