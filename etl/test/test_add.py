import unittest
from src import add


class TestAdd(unittest.TestCase):
    def test_neg_pos_add(self):
        self.assertEqual(add.add(-1, 1), 0)

    def test_neg_add(self):
        self.assertEqual(add.add(-1, -1), -2)

    def test_pos_add(self):
        self.assertEqual(add.add(1, 2), 3)

    def test_big_add(self):
        self.assertEqual(add.add(12345, 12345), 24690)


if __name__ == "__main__":
    unittest.main()
