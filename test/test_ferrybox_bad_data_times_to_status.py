import unittest

import ferrybox_bad_data_times_to_status
import datetime


def text_to_dt(t):
    return datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')

class TestFerryboxBadDataTimesToStatus(unittest.TestCase):
    # def test_standard(self):
    #     """Test output of standard rows"""
    #     result = ferrybox_bad_data_times_to_status.process_file(input_file='test_pump_standard.csv')
    #
    #     tempfile_name = tempfile.NamedTemporaryFile(suffix='.test')
    #     ferrybox_bad_data_times_to_status.list_to_csv(result, tempfile_name.name)
    #
    #     expected = list(io.open('test_pump_standard_output.csv'))
    #     actual = list(io.open(tempfile_name.name))
    #
    #     self.assertListEqual(actual, expected)
    #
    # def test_minute(self):
    #     """Test rows with same start and end to nearest minute"""
    #     result = ferrybox_bad_data_times_to_status.process_file(input_file='test_pump_minute.csv')
    #
    #     tempfile_name = tempfile.NamedTemporaryFile(suffix='.test')
    #     ferrybox_bad_data_times_to_status.list_to_csv(result, tempfile_name.name)
    #
    #     expected = list(io.open('test_pump_minute_output.csv'))
    #     actual = list(io.open(tempfile_name.name))
    #
    #     self.assertListEqual(actual, expected)
    #
    # def test_second(self):
    #     """Test rows with same start and end to nearest second"""
    #     result = ferrybox_bad_data_times_to_status.process_file(input_file='test_pump_second.csv')
    #
    #     tempfile_name = tempfile.NamedTemporaryFile(suffix='.test')
    #     ferrybox_bad_data_times_to_status.list_to_csv(result, tempfile_name.name)
    #
    #     expected = list(io.open('test_pump_second_output.csv'))
    #     actual = list(io.open(tempfile_name.name))
    #
    #     self.assertListEqual(actual, expected)
    #
    # def test_midnight(self):
    #     """Test where there are two or more rows that span a midnight."""
    #     result = ferrybox_bad_data_times_to_status.process_file(input_file='test_pump_midnight.csv')
    #
    #     tempfile_name = tempfile.NamedTemporaryFile(suffix='.test')
    #     ferrybox_bad_data_times_to_status.list_to_csv(result, tempfile_name.name)
    #
    #     expected = list(io.open('test_pump_midnight_output.csv'))
    #     actual = list(io.open(tempfile_name.name))
    #
    #     self.assertListEqual(actual, expected)

    def test_combine_multiday_rows(self):
        pump_log = [['2016-12-25', '23:45:00', '23:50:00'],
                    ['2016-12-26', '06:10:00', '20:40:00'],
                    ['2016-12-27', '19:00:00', '20:00:00'],
                    ]
        expected = [[text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:50:00'), 'off'],
                    [text_to_dt('2016-12-25 23:50:00'), text_to_dt('2016-12-26 06:10:00'), 'on'],
                    [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 20:40:00'), 'off'],
                    [text_to_dt('2016-12-26 20:40:00'), text_to_dt('2016-12-27 19:00:00'), 'on'],
                    [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-27 20:00:00'), 'off'],
                    ]

        actual = ferrybox_bad_data_times_to_status.combine_multiday_rows(pump_log)

        self.assertListEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
