import unittest
import datetime
import ferrybox_bad_data_times_to_status
import io
import tempfile



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
    def test_midnight(self):
        """Test where there are two or more rows that span a midnight."""
        result = ferrybox_bad_data_times_to_status.process_file(input_file='test_pump_midnight.csv')

        tempfile_name = tempfile.NamedTemporaryFile(suffix='.test')
        ferrybox_bad_data_times_to_status.list_to_csv(result, tempfile_name.name)

        expected = list(io.open('test_pump_midnight_output.csv'))
        actual = list(io.open(tempfile_name.name))

        self.assertListEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()