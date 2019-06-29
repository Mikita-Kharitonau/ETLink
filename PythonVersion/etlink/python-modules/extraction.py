import luigi
import luigi.contrib.hdfs

import urllib.request
from urllib.error import HTTPError
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from luigi.contrib.spark import PySparkTask
from config import (
    APPLICATION_NAME,
    EXTRACTION_OUTPUT,
    HDFS_DEFAULT_NAME
)


ERROR_MESSAGE = "Can not download data from {url}:\n{error}"

# use dictionary with functions to prevent using multiple if-else constructions.
STARTS_WITH_CASES = {
        "/": lambda self, link: self.starts_with_slash(link),
        "#": lambda self, link: self.starts_with_sharp(link),
        "h": lambda self, link: self.starts_with_http(link)
}


class Extraction(PySparkTask):
    """
    Extraction task.
    Get all links from a self.url web page and store them in file on hdfs.
    """

    url = luigi.Parameter()

    def requires(self):
        """
        First task in chain, does not depend on anyone.
        """

        return []

    def get_output_name(self):
        """
        Creates a path to save file with links, extracted from "self.url".
        """

        return "{hdfs}/{app_name}/{folder}/{file}".format(
            hdfs=HDFS_DEFAULT_NAME,
            app_name=APPLICATION_NAME,
            folder=EXTRACTION_OUTPUT,
            file=str(self.url).replace('/', '-').replace(":", "")
        )

    def output(self):
        """
        Returns hdfs target for output file.
        """

        return luigi.contrib.hdfs.HdfsTarget(self.get_output_name())

    def run(self):
        """
        Extracts all links from "self.utl" and put them in output file.
        """

        with self.output().open('w') as f:
            try:
                response = urllib.request.urlopen(self.url)
                html = response.read()
            except ValueError as e:
                f.write(ERROR_MESSAGE.format(url=self.url, error=e))
                print(ERROR_MESSAGE.format(url=self.url, error=e))
                return
            except HTTPError as e:
                html = e.read()

            links = self.extract(str(html))
            self.save_to_output(f, links)

    def save_to_output(self, out_file, links):
        for link in links:
            out_file.write(link + '\n')

    def extract(self, html):
        """
        Extracts all links using BeautifulSoup.
        :return: list of links.
        """

        links = []
        soup = BeautifulSoup(html)
        for tag_a in soup.find_all('a'):
            link = tag_a.get('href')
            if link:
                # we should check fist symbol, to handle links of the form
                # "/wiki/Slaget_ved_Austerlitz"
                # or
                # "#cite_ref-controversy_61-1"
                link = STARTS_WITH_CASES.get(link[0], lambda s, l: link)(self, link)
                links.append(link)

        return links

    def starts_with_slash(self, link):
        """
        If link starts with "/":
        Example:
             link = /wiki/Alexander_I_of_Russia
             self.url = https://en.wikipedia.org/wiki/Battle_of_Austerlitz
             :returns https://en.wikipedia.org/wiki/Alexander_I_of_Russia
        """

        parsed = urlparse(self.url)
        return str(self.url).replace(parsed.path, "") + link

    def starts_with_sharp(self, link):
        """
        If link starts with "#", returns "self.url" + link.
        Example:
             link = #Preliminary_moves
             self.url = https://en.wikipedia.org/wiki/Battle_of_Austerlitz
             :returns https://en.wikipedia.org/wiki/Battle_of_Austerlitz#Preliminary_moves
        """

        return str(self.url) + link

    def starts_with_http(self, link):
        """
        If link starts with "h", it means that link is valid.
        Example:
             link = https://en.wikipedia.org/wiki/Battle_of_Austerlitz
             :returns https://en.wikipedia.org/wiki/Battle_of_Austerlitz
        """

        return link