import unittest
import subprocess

from extraction import Extraction
from config import APPLICATION_NAME

TEST_URL = "https://en.wikipedia.org/wiki/Napoleon"

class TestExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.extraction_task = Extraction(url=TEST_URL)
        cls.cmd("hadoop fs -rm -r /{app_name}".format(app_name=APPLICATION_NAME).split())

    @classmethod
    def tearDownClass(cls):
        cls.cmd("hadoop fs -rm -r /{app_name}".format(app_name=APPLICATION_NAME).split())

    @classmethod
    def cmd(cls, args):
        print("Test command: {cmd}".format(cmd=" ".join(args)))
        proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        proc.communicate()

        return proc.returncode


    def test_setting_url(self):
        self.assertEqual(self.extraction_task.url, TEST_URL)

    def test_extraction(self):
        import os
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "test-resources/extraction_test.html"), "r") as in_file:
            html = in_file.read()

        expected_links = [
            "https://en.wikipedia.org/wiki/Napoleon#mw-head",
            "https://en.wikipedia.org/wiki/Napoleon#p-search",
            "https://en.wikipedia.org/wiki/Napoleon_III",
            "https://en.wikipedia.org/wiki/Napoleon_(disambiguation)",
            "https://en.wikipedia.org/wiki/Napoleon_Bonaparte_(disambiguation)",
            "https://en.wikipedia.org/wiki/Emperor_of_the_French",
            "https://en.wikipedia.org/wiki/Coronation_of_Napoleon_I",
            "https://en.wikipedia.org/wiki/Notre-Dame_Cathedral",
            "https://en.wikipedia.org/wiki/Napoleon_Bonaparte%27s_battle_record"
        ]

        actual_links = self.extraction_task.extract(html)

        self.assertEqual(len(actual_links), len(expected_links))

        self.assertFalse(set(actual_links) - set(expected_links))

    def test_saving_to_output(self):

        test_links = [
            "https://en.wikipedia.org/wiki/Napoleon#mw-head",
            "https://en.wikipedia.org/wiki/Napoleon#p-search",
            "https://en.wikipedia.org/wiki/Napoleon_III"
        ]
        args = ["hadoop", "fs", "-test", "-e", self.extraction_task.output().path]

        self.assertTrue(self.cmd(args))
        with self.extraction_task.output().open("w") as out_file:
            self.extraction_task.save_to_output(out_file, test_links)
        self.assertFalse(self.cmd(args))
        with self.extraction_task.output().open("r") as in_file:
            for line, link in zip(in_file, test_links):
                self.assertEqual(link, line.replace("\n", ""))
