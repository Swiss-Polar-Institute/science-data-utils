import get_bottle_firing_times

import unittest
import datetime
import tempfile

class TestGetBottleFiringPositions(unittest.TestCase):
    def test_read_bottles_date_time(self):
        bottles_date_time = get_bottle_firing_times.get_bottles_datetime("ctd-bottle-firing.btl")

        self.assertEqual(len(bottles_date_time), 2)
        self.assertEqual(bottles_date_time[1], datetime.datetime(2017, 12, 28, 12, 58, 22))
        self.assertEqual(bottles_date_time[2], datetime.datetime(2017, 12, 29, 11, 44, 33))

    def test_read_bottles_date_time_one_column_less(self):
        bottles_date_time = get_bottle_firing_times.get_bottles_datetime("ctd-bottle-firing-one-less-column.btl")

        self.assertEqual(len(bottles_date_time), 2)
        self.assertEqual(bottles_date_time[1], datetime.datetime(2017, 12, 28, 12, 58, 22))
        self.assertEqual(bottles_date_time[2], datetime.datetime(2017, 12, 29, 11, 44, 33))

    def test_read_bottles_assert_two_avg(self):
        self.assertRaises(AssertionError, get_bottle_firing_times.get_bottles_datetime, "ctd-bottle-firing-two-avg.btl")

    def test_read_bottles_assert_no_avg_sdev(self):
        self.assertRaises(AssertionError, get_bottle_firing_times.get_bottles_datetime, "lines-no-avg-sdev.btl")

    def test_write_to_file(self):
        bottles = {}
        bottles[1] = "2019-02-12 11:22:33"
        bottles[2] = "2019-03-14 12:21:34"

        output_filename = tempfile.NamedTemporaryFile()

        get_bottle_firing_times.write_to_file(output_filename.name, bottles)

        file_contents = open(output_filename.name).readlines()

        self.assertEqual(file_contents,
                         ["BTLNBR,date_time\n",
                          "1,2019-02-12 11:22:33\n",
                          "2,2019-03-14 12:21:34\n"])