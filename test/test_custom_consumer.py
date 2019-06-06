"""Unit test custom consumer"""
import unittest

from kombu import Exchange

from skinos.custom_consumer import CustomConsumer


class TestCustomConsumer(unittest.TestCase):

    def test_with_sentry__ok(self):
        self.assertEqual(CustomConsumer.with_sentry(True, True), (True, True))
        self.assertEqual(CustomConsumer.with_sentry(True, False), (True, False))
        self.assertEqual(CustomConsumer.with_sentry(False, True), (False, True))
        self.assertEqual(CustomConsumer.with_sentry(False, False), (False, False))

    def test_with_sentry__fail(self):

        with self.assertRaises(TypeError):
            CustomConsumer.with_sentry("not a bool", True)

        with self.assertRaises(TypeError):
            CustomConsumer.with_sentry(True, "not a bool")

    def test_add_exchange_ok(self):
        ex_name = 'test'
        bk = '#'
        ex = CustomConsumer.add_exchange(ex_name, bk)

        self.assertIsInstance(ex, Exchange)
        self.assertEqual(ex.name, ex_name)

    def test_add_exchange_raise(self):
        with self.assertRaises(RuntimeError):
            CustomConsumer.add_exchange('test', '#')


