import unittest

import ferrybox_bad_data_times_to_status
import datetime


def text_to_dt(t):
    """Use the function to convert date time strings to python datetime format in the input and expected output data
    in the tests below. """

    return datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')

class TestFerryboxBadDataTimesToStatus(unittest.TestCase):
    maxDiff = None # allows full output of any failed tests to be printed

    def test_change_format_from_input_to_datetime(self):
        """Test simple conversion from input  off periods in format date, time, time to python datetime"""

        pump_log_input = [['2016-12-24', '11:30:00', '12:30:00'],
                    ['2016-12-25', '23:45:00', '23:50:00'],
                    ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                    [text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:50:00')],
                    ]

        actual = ferrybox_bad_data_times_to_status.change_format_from_input_to_datetime(pump_log_input)

        self.assertListEqual(actual, expected)


    def test_collapse_same_day_simple(self):
        """Test code to convert mulitple lines that run where the time periods could be combined (such as over
        multiple days) because there are no gaps.

        First test of simple rows where this case is not included. """

        off_input = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
               [text_to_dt('2016-12-26 01:00:00'), text_to_dt('2016-12-26 14:30:00')],
               [text_to_dt('2016-12-27 13:00:00'), text_to_dt('2016-12-27 15:45:00')],
               ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
               [text_to_dt('2016-12-26 01:00:00'), text_to_dt('2016-12-26 14:30:00')],
               [text_to_dt('2016-12-27 13:00:00'), text_to_dt('2016-12-27 15:45:00')],
               ]

        actual = ferrybox_bad_data_times_to_status.collapse_same_day_off(off_input)

        self.assertListEqual(actual, expected)


    def test_collapse_same_day_off(self):
        """Test code to convert mulitple lines that run where the time periods could be combined (such as over
        multiple days) because there are no gaps.

        Test where there are only two rows that should be combined into one. """

        off_input = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                    [text_to_dt('2016-12-25 20:45:00'), text_to_dt('2016-12-25 23:59:59')],
                    [text_to_dt('2016-12-26 00:00:00'), text_to_dt('2016-12-26 14:30:00')],
                    [text_to_dt('2016-12-27 13:00:00'), text_to_dt('2016-12-27 15:45:00')],
                    ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                    [text_to_dt('2016-12-25 20:45:00'), text_to_dt('2016-12-26 14:30:00')],
                    [text_to_dt('2016-12-27 13:00:00'), text_to_dt('2016-12-27 15:45:00')],
                    ]

        actual = ferrybox_bad_data_times_to_status.collapse_same_day_off(off_input)

        self.assertListEqual(actual, expected)


    def test_collapse_same_day_off_multi(self):
        """Test code to convert mulitple lines that run where the time periods could be combined (such as over
        multiple days) because there are no gaps.

        Test where there are multiple rows that should be combined into one row. """

        off_input = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
               [text_to_dt('2016-12-25 20:45:00'), text_to_dt('2016-12-25 23:59:59')],
               [text_to_dt('2016-12-26 00:00:00'), text_to_dt('2016-12-26 23:59:59')],
               [text_to_dt('2016-12-27 00:00:00'), text_to_dt('2016-12-27 14:30:00')],
               [text_to_dt('2016-12-28 13:00:00'), text_to_dt('2016-12-28 15:45:00')],
               ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                    [text_to_dt('2016-12-25 20:45:00'), text_to_dt('2016-12-27 14:30:00')],
                    [text_to_dt('2016-12-28 13:00:00'), text_to_dt('2016-12-28 15:45:00')],
                    ]

        actual = ferrybox_bad_data_times_to_status.collapse_same_day_off(off_input)

        self.assertListEqual(actual, expected)


    def test_collapse_same_day_off_one_consecutive(self):
        """Test code to convert mulitple lines that run where the time periods could be combined (such as over multiple
        days) because there are no gaps.

        Test where there are two rows that should be combined into one row, but because they are consecutive rather
        than run over midnight and consecutive. """

        off_input = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                     [text_to_dt('2016-12-25 12:40:01'), text_to_dt('2016-12-25 12:50:10')],
                     [text_to_dt('2016-12-25 12:50:11'), text_to_dt('2016-12-25 13:30:00')],
                     [text_to_dt('2016-12-26 08:00:00'), text_to_dt('2016-12-26 15:00:00')]
                    ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                     [text_to_dt('2016-12-25 12:40:01'), text_to_dt('2016-12-25 13:30:00')],
                     [text_to_dt('2016-12-26 08:00:00'), text_to_dt('2016-12-26 15:00:00')]
                    ]

        actual = ferrybox_bad_data_times_to_status.collapse_same_day_off(off_input)

        self.assertListEqual(actual, expected)


    def test_collapse_same_day_off_multi_consecutive(self):
        """Test code to convert mulitple lines that run where the time periods could be combined (such as over multiple days) because there are no gaps.

        Test where there are multiple rows that should be combined into one row, but because they are consecutive
        rather than run over midnight and consecutive. """

        off_input = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                     [text_to_dt('2016-12-25 12:40:01'), text_to_dt('2016-12-25 12:50:10')],
                     [text_to_dt('2016-12-25 12:50:11'), text_to_dt('2016-12-25 13:30:05')],
                     [text_to_dt('2016-12-25 13:30:06'), text_to_dt('2016-12-25 16:00:00')],
                     [text_to_dt('2016-12-26 08:00:00'), text_to_dt('2016-12-26 15:00:00')]
                     ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                    [text_to_dt('2016-12-25 12:40:01'), text_to_dt('2016-12-25 16:00:00')],
                    [text_to_dt('2016-12-26 08:00:00'), text_to_dt('2016-12-26 15:00:00')]
                    ]

        actual = ferrybox_bad_data_times_to_status.collapse_same_day_off(off_input)

        self.assertListEqual(actual, expected)



    def test_correct_off_seconds_same_minute(self):
        """Test code where the off periods have the same start and end time to the nearest minute. The end time of
        the off period in these cases, should have 59 seconds added to them. Test covers the cases where there are no
        rows that meet these criteria and also rows where the start and end is the same but to the nearest second."""

        minute_input =  [[text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:45:00')],
                         [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 06:10:00')],
                         [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-27 20:00:00')],
                         [text_to_dt('2016-12-28 21:00:05'), text_to_dt('2016-12-28 21:00:05')]
                         ]

        expected = [[text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:45:59')],
                    [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 06:10:59')],
                    [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-27 20:00:00')],
                    [text_to_dt('2016-12-28 21:00:05'), text_to_dt('2016-12-28 21:00:05')]
                    ]

        actual = ferrybox_bad_data_times_to_status.correct_off_seconds_same_minute(minute_input)

        self.assertListEqual(actual, expected)


    def test_process_to_on_off(self):
        """Test the code that converts lines of off periods to on and off rows.

        First simple case. """

        off_input = [[text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:50:00')],
                     [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 20:40:00')],
                     [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-27 20:00:00')]
                     ]

        expected = [[text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:50:00'), 'off'],
                    [text_to_dt('2016-12-25 23:50:00'), text_to_dt('2016-12-26 06:10:00'), 'on'],
                    [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 20:40:00'), 'off'],
                    [text_to_dt('2016-12-26 20:40:00'), text_to_dt('2016-12-27 19:00:00'), 'on'],
                    [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-27 20:00:00'), 'off'],
                    ]

        actual = ferrybox_bad_data_times_to_status.process_to_on_off(off_input)

        self.assertListEqual(actual, expected)


    def test_combine_multiday_rows_join(self):
        """Test the code that converts lines of off periods to on and off rows.

            Case where there are rows to combine. """
        off_input = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00')],
                     [text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:50:00')],
                     [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-27 19:30:00')],
                     [text_to_dt('2016-12-28 20:00:00'), text_to_dt('2016-12-28 21:00:00')],
                     ]

        expected = [[text_to_dt('2016-12-24 11:30:00'), text_to_dt('2016-12-24 12:30:00'), 'off'],
                    [text_to_dt('2016-12-24 12:30:00'), text_to_dt('2016-12-25 23:45:00'), 'on'],
                    [text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:50:00'), 'off'],
                    [text_to_dt('2016-12-25 23:50:00'), text_to_dt('2016-12-26 06:10:00'), 'on'],
                    [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-27 19:30:00'), 'off'],
                    [text_to_dt('2016-12-27 19:30:00'), text_to_dt('2016-12-28 20:00:00'), 'on'],
                    [text_to_dt('2016-12-28 20:00:00'), text_to_dt('2016-12-28 21:00:00'), 'off'],
                    ]

        actual = ferrybox_bad_data_times_to_status.process_to_on_off(off_input)

        self.assertListEqual(actual, expected)


    def test_process_seconds_and_to_on_off(self):
        """Tests the process of combining rows, converting the seconds of the end of an off row and processing to on and off rows."""

        minute_input = [[text_to_dt('2016-12-25 11:00:00'), text_to_dt('2016-12-25 12:00:00')],
                        [text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:45:00')],
                        [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 06:10:00')],
                        [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-27 23:59:59')],
                        [text_to_dt('2016-12-28 00:00:00'), text_to_dt('2016-12-28 21:00:05')],
                        [text_to_dt('2016-12-28 21:00:06'), text_to_dt('2016-12-28 22:00:00')],
                        [text_to_dt('2016-12-29 08:00:00'), text_to_dt('2016-12-29 12:00:00')]
                        ]

        expected = [[text_to_dt('2016-12-25 11:00:00'), text_to_dt('2016-12-25 12:00:00'), 'off'],
                        [text_to_dt('2016-12-25 12:00:00'), text_to_dt('2016-12-25 23:45:00'), 'on'],
                        [text_to_dt('2016-12-25 23:45:00'), text_to_dt('2016-12-25 23:45:59'), 'off'],
                        [text_to_dt('2016-12-25 23:45:59'), text_to_dt('2016-12-26 06:10:00'), 'on'], # the start time here needs correcting when the code changes
                        [text_to_dt('2016-12-26 06:10:00'), text_to_dt('2016-12-26 06:10:59'), 'off'],
                        [text_to_dt('2016-12-26 06:10:59'), text_to_dt('2016-12-27 19:00:00'), 'on'], # the start time here needs correcting when the code changes
                        [text_to_dt('2016-12-27 19:00:00'), text_to_dt('2016-12-28 22:00:00'), 'off'],
                        [text_to_dt('2016-12-28 22:00:00'), text_to_dt('2016-12-29 08:00:00'), 'on'],
                        [text_to_dt('2016-12-29 08:00:00'), text_to_dt('2016-12-29 12:00:00'), 'off']
                        ]

        collapsed_list = ferrybox_bad_data_times_to_status.collapse_same_day_off(minute_input)
        correct_seconds = ferrybox_bad_data_times_to_status.correct_off_seconds_same_minute(collapsed_list)
        actual = ferrybox_bad_data_times_to_status.process_to_on_off(correct_seconds)


        self.assertListEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()
