from twisted.trial import unittest
from lighthouse import util


class MoveToFrontTests(unittest.TestCase):
    def test_move_to_front(self):
        fake_l = [('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)]
        self.assertEqual([('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)], util.move_to_front('three', fake_l))

    def test_move_to_front_when_already_at_front(self):
        fake_l = [('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)]
        self.assertEqual([('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)], util.move_to_front('three', fake_l))

    def test_move_to_front_when_not_in_list(self):
        fake_l = [('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)]
        self.assertEqual([('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)],
                         util.move_to_front('six', fake_l))


class AddToFrontTests(unittest.TestCase):
    def test_add_to_front(self):
        fake_l = [('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)]
        self.assertEqual([('six', 6), ('one', 1), ('two', 2), ('three', 3), ('four', 4)],
                         util.add_to_front(('six', 6), fake_l))

    def test_add_to_front_when_already_at_front(self):
        fake_l = [('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)]
        self.assertEqual(fake_l, util.add_to_front(('one', 1), fake_l))

    def test_add_to_front_when_in_wrong_position(self):
        fake_l = [('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)]
        self.assertEqual(fake_l, util.add_to_front(('two', 2), fake_l))


class AddOrMoveMoveToFrontTests(unittest.TestCase):
    def test_add_or_move_to_front_when_not_at_front(self):
        fake_l = [('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)]
        self.assertEqual([('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)], util.add_or_move_to_front('three', fake_l))

    def test_add_or_move_to_front_when_already_at_front(self):
        fake_l = [('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)]
        self.assertEqual([('three', 3), ('one', 1), ('two', 2), ('four', 4), ('five', 5)], util.add_or_move_to_front('three', fake_l))

    def test_add_or_move_to_front_when_not_in_list(self):
        fake_l = [('one', 1), ('two', 2), ('three', 3), ('four', 4), ('five', 5)]
        self.assertEqual([('six', 1000), ('one', 1), ('two', 2), ('three', 3), ('four', 4)], util.add_or_move_to_front('six', fake_l))