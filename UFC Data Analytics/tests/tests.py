import unittest
from spark.spark_etl import height_to_cm, inch_to_cm


class TestHeightConverter(unittest.TestCase):
    def test_height_valid(self):
        """
        Test if valid height input with feet and inches provided is extracted and converted into cm
        """
        result = height_to_cm("5' 10\"")
        self.assertEqual(result, 178)

    def test_height_feet_only(self):
        """
        Test if height input without inches is extracted and converted into cm
        """
        result = height_to_cm("6'")
        self.assertEqual(result, 183)

    def test_height_invalid_input(self):
        """
        Test if height input without inches is extracted and converted into cm
        """
        result = height_to_cm("--")
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
