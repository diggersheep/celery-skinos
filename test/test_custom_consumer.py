"""Unit test custom consumer"""
import unittest

from kombu import Queue
from uuid import uuid4

from kombu import Exchange

from skinos.custom_consumer import CustomConsumer


class Message:
    """MOCK"""
    @staticmethod
    def ack():
        pass


class App:
    """MOCK"""

    class Add:
        data = []

        def add(self, data):
            self.data.append(data)

    steps = {"consumer": Add()}

    def __init__(self):
        pass



class TestCustomConsumer(unittest.TestCase):

    def setUp(self):
        CustomConsumer.count = 0
        CustomConsumer.exchanges = {}
        CustomConsumer.queues = {}
        CustomConsumer.meta_consumers = {}

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
        CustomConsumer.add_exchange('test', '#')
        with self.assertRaises(RuntimeError):
            CustomConsumer.add_exchange('test', '#')

    def test__hello_message_ok(self):
        """do nothing specific. Sorry it's for coverage ^^"""
        self.assertIsNone(CustomConsumer._hello_message())

    def test__message_handler_builder__function_with_less_than_two_agrs(self):
        with self.assertRaises(SystemExit):
            CustomConsumer._message_handler_builder(lambda a: a)

    def test__message_handler_builder__fun_wrapper__ok(self):
        fun = lambda a, b: 10
        fun_true = lambda a, b: True
        fun_false = lambda a, b: False

        fun_wrapper = CustomConsumer._message_handler_builder(fun)
        result = fun(None, Message())
        self.assertEqual(result, fun_wrapper(None, Message()))

        fun_wrapper = CustomConsumer._message_handler_builder(fun_true)
        result = fun_true(None, Message())
        self.assertEqual(result, fun_wrapper(None, Message()))

        fun_wrapper = CustomConsumer._message_handler_builder(fun_false)
        result = fun_false(None, Message())
        self.assertEqual(result, fun_wrapper(None, Message()))

    def test__message_handler_builder__fun_wrapper__raise(self):
        fun_wrapper = CustomConsumer._message_handler_builder(lambda a, b: 0/0)
        with self.assertRaises(ZeroDivisionError):
            fun_wrapper(None, Message())

    def test__consumer_builder__ok(self):
        exchange_name = str(uuid4())
        queue_name = str(uuid4())
        routing_key = str(uuid4())
        entry_name = f"{exchange_name}{CustomConsumer.sep}{queue_name}"
        ex = CustomConsumer.add_exchange(exchange_name, routing_key)

        @CustomConsumer.consumer(exchange_name, queue_name, routing_key)
        def fun(a, b):
            return 10

        self.assertEqual(CustomConsumer.count, 1)
        self.assertEqual(len(CustomConsumer.queues), 1)
        self.assertIn(entry_name, CustomConsumer.queues)
        self.assertIsInstance(CustomConsumer.queues[entry_name], Queue)

        self.assertIn(entry_name, CustomConsumer.meta_consumers)
        meta_consumer = CustomConsumer.meta_consumers[entry_name]
        self.assertIsInstance(meta_consumer, dict)
        for key in ["callbacks", "exchange_name", "queue_name", "binding_key", "consumer_id", "task_name", "accept"]:
            self.assertIn(key, meta_consumer)

        self.assertEqual(len(meta_consumer["callbacks"]), 1)
        self.assertEqual(meta_consumer["exchange_name"], exchange_name)
        self.assertEqual(meta_consumer["queue_name"], queue_name)
        self.assertEqual(meta_consumer["binding_key"], routing_key)
        self.assertEqual(meta_consumer["consumer_id"], CustomConsumer.count - 1)

    def test__consumer_builder__raise(self):
        exchange_name = str(uuid4())
        queue_name = str(uuid4())
        routing_key = str(uuid4())

        CustomConsumer.add_exchange(exchange_name, routing_key)

        with self.assertRaises(SystemExit):
            @CustomConsumer.consumer(exchange_name, queue_name, routing_key)
            def fun(a, b):
                return 10

            @CustomConsumer.consumer(exchange_name, queue_name, routing_key)
            def fun2(a, b):
                return 10

    def test__consumer_builder(self):
        exchange_name = str(uuid4())
        queue_name = str(uuid4())
        routing_key = str(uuid4())
        entry_name = f"{exchange_name}{CustomConsumer.sep}{queue_name}"

        ex = CustomConsumer.add_exchange(exchange_name, routing_key)

        @CustomConsumer.consumer(exchange_name, queue_name, routing_key)
        def fun(a, b):
            return 10

        consumers = CustomConsumer._consumer_builder(None)

        self.assertEqual(len(consumers), 1)
        consumer = consumers[0]
        self.assertEqual(len(consumer.queues), 1)
        self.assertEqual(consumer.queues[0].name, queue_name)

        for s in ["application/json", "text/plain"]:
            self.assertIn(s, consumer.accept)


    def test__build(self):
        exchange_name = str(uuid4())
        queue_name = str(uuid4())
        routing_key = str(uuid4())
        entry_name = f"{exchange_name}{CustomConsumer.sep}{queue_name}"

        ex = CustomConsumer.add_exchange(exchange_name, routing_key)

        @CustomConsumer.consumer(exchange_name, queue_name, routing_key)
        def fun(a, b):
            return 10

        app = App()
        CustomConsumer.build(app)

        self.assertEqual(app.steps["consumer"].data[0].name, "CustomConsumer")
