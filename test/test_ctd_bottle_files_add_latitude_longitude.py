import ctd_bottle_files_add_latitude_longitude

import unittest
import datetime
import tempfile
import filecmp
import os

class TestGetBottleFiringPositions(unittest.TestCase):
    def test_add_positions_to_bottles(self):
        output_file = tempfile.NamedTemporaryFile(delete=False)
        output_file.close()
        ctd_bottle_files_add_latitude_longitude.add_latitude_longitude("ctd-bottle-firing.btl", output_file.name)

        self.assertTrue(filecmp.cmp(output_file.name, "ctd-bottle-firing-positions-added.btl"))

        # os.unlink(output_file.name)


