import functools
import types
import logging as logger

import uuid
from kombu import Exchange, Queue, Consumer
from kombu.exceptions import DecodeError
from celery import bootsteps


class CustomConsumer:
    exchanges = {}
    queues = {}
    consumers = {}
    count = 0

    @classmethod
    def add_exchange(cls, exchange_name, exchange_type, routing_key):
        '''
        Add exchange to an internal list (don't add if the exchange exists)
        Declare the exchange

        :param exchange_name: name of exchange
        :type exchange_name: str
        :param exchange_type:
        :type exchange_type: str
        :param routing_key:
        :return:
        '''
        if not exchange_name in cls.exchanges:
            cls.exchanges[exchange_name] = Exchange(exchange_name, type=exchange_type, routing_key=routing_key)
            logger.debug('New exchnange (exchange: {}, routing_key: {})'.format(exchange_name, routing_key))

    @classmethod
    def consumer(cls, app, exchange_name, queue_name, binding_key):
        '''
        :param app:
        :param exchange_name:
        :param queue_name:
        :param binding_key:
        :return:
        '''

        def f(fun):
            queue_key = '{ex}.{q}'.format(ex=exchange_name, q=queue_name)

            if not queue_key in cls.queues:
                cls.queues[queue_key] = Queue(
                    queue_name,
                    cls.exchanges[exchange_name],
                    binding_key
                )

            consumer_name = 'Consumer{}'.format(cls.count)

            def get_consumers(self, ch):
                return [Consumer(ch,
                                 queues=[cls.queues[queue_key]],
                                 callbacks=[self.handle_message],
                                 accept=['json'])]

            def handle_message(self, body, message):
                result = fun(body, message)
                # anyway consume the message (no ack = message stayed in queue = bad !)
                message.ack()
                return result

            cls.consumers[consumer_name] = type(
                consumer_name,
                (bootsteps.ConsumerStep,),
                {
                    'get_consumers': get_consumers,
                    'handle_message': handle_message
                }
            )

            app.steps['consumer'].add(cls.consumers[consumer_name])

            cls.count += 1
            return fun

        return f


class MultiConsumer:
    exchanges = {}
    queues = {}
    meta_consumers = {}
    count = 0
    sep = '|'

    # meta_consumers = {
    #     'exchange.queue': {
    #         'exchange_name': '',
    #         'queue_name': '',
    #         'callbacks': []
    #     }
    # }

    @classmethod
    def add_exchange(cls, exchange_name, routing_key='#'):
        """
        Add exchange to an internal list (don't add if the exchange exists)
        Declare the exchange

        :param exchange_name: name of exchange
        :type exchange_name: str
        :param exchange_type:
        :param routing_key:
        :return:
        """

        if exchange_name not in cls.exchanges:
            cls.exchanges[exchange_name] = Exchange(exchange_name, type='topic', routing_key=routing_key)

    @classmethod
    def consumer(cls, exchange_name, queue_name, binding_key):
        '''
        :param app:
        :param exchange_name:
        :param queue_name:
        :param binding_key:
        :return:
        '''

        def f(fun):
            entry_name = '{ex}{sep}{q}{sep}{bk}'.format(ex=exchange_name, q=queue_name, bk=binding_key, sep=cls.sep)
            if not entry_name in cls.queues:
                cls.queues[entry_name] = Queue(
                    queue_name,
                    cls.exchanges[exchange_name],
                    binding_key
                )
                logger.debug('queue created (exchange: {ex}, queue: {q}, binding_key: {bk})'.format(
                    ex=exchange_name,
                    q=queue_name,
                    bk=binding_key,
                ))

            # ex.queue.fun_name
            if entry_name not in cls.meta_consumers:
                cls.meta_consumers[entry_name] = {
                    'callbacks': [],
                    'queue_name': queue_name,
                    'exchange_name': exchange_name,
                    'binding_key': binding_key,
                    'consumer_id': cls.count,
                    'accept': ['json']
                }
            cls.meta_consumers[entry_name]['callbacks'].append(cls.__message_handler_builder(fun))
            cls.count += 1

            return fun

        return f

    @classmethod
    def build(cls, app):

        def get_consumers(self, ch):
            return cls.__consumer_builder(ch)

        consumer_step = type(
            'CustomConsumer',
            (bootsteps.ConsumerStep,),
            {
                'get_consumers': get_consumers,
            }
        )

        app.steps['consumer'].add(consumer_step)



    @classmethod
    def __message_handler_builder(cls, function):
        #@functools.wraps
        def wrapper(body, msg):
            result = function(body, msg)
            msg.ack()
            return result

        wrapper.__name__ = 'wrapper_{}'.format(str(uuid.uuid4()).replace('-', ''))
        logger.warn('new message handler {}'.format(wrapper.__name__))
        return wrapper

    @classmethod
    def __consumer_builder(cls, ch):
        consumers = []
        for consumer_name, meta_consumer in cls.meta_consumers.items():
            queue_key = '{ex}{sep}{q}{sep}{bk}'.format(
                ex=meta_consumer['exchange_name'],
                q=meta_consumer['queue_name'],
                bk=meta_consumer['binding_key'],
                sep=cls.sep
            )
            consumers.append(Consumer(
                channel=ch,
                queues=[cls.queues[queue_key]],
                callbacks=meta_consumer['callbacks'],
                accept=meta_consumer['accept']
            ))
            logger.info('Consumer created (exchange: {ex}, queue: {q}, binding_key: {bk}, # callbacks: {n})'.format(
                ex=meta_consumer['exchange_name'],
                q=meta_consumer['queue_name'],
                bk=meta_consumer['binding_key'],
                n=len(meta_consumer['callbacks']),
            ))

        return consumers