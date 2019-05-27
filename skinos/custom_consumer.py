"""Skinos"""

from typing import Dict
import logging as logger

import uuid
from kombu import Exchange, Queue, Consumer
from celery import bootsteps


class MultiConsumer:
    """Class for multiConsumer"""
    exchanges: Dict[str, object] = {}
    queues = {}
    meta_consumers = {}
    count = 0
    sep = '|'
    _with_sentry = False

    # format for meta_consumers
    # meta_consumers = {
    #     'exchange.queue': {
    #         'exchange_name': '',
    #         'queue_name': '',
    #         'callbacks': [one_function]
    #     }
    # }

    @classmethod
    def with_sentry(cls, __with_sentry: bool) -> bool:
        """
        set sentry
        :param b: bool
        :return: with sentry boolean
        """
        if not isinstance(__with_sentry, bool):
            cls._with_sentry = False
            raise TypeError('Got type {}, expected bool.'.format(type(__with_sentry)))
        cls._with_sentry = __with_sentry
        return __with_sentry

    @classmethod
    def add_exchange(cls, exchange_name: str, routing_key: str = '#'):
        """
        Add exchange to an internal list (don't add if the exchange exists)
        Declare the exchange

        :param exchange_name: name of exchange
        :type exchange_name: str
        :param routing_key:
        :return:
        """

        if exchange_name not in cls.exchanges:
            cls.exchanges[exchange_name] = Exchange(
                exchange_name,
                type='topic',
                routing_key=routing_key
            )

    @classmethod
    def consumer(cls, exchange_name: str, queue_name: str, binding_key: str):
        """
        :param app:
        :param exchange_name:
        :param queue_name:
        :param binding_key:
        :return:
        """

        def _sub_fun(fun):
            entry_name = '{ex}{sep}{q}'.format(ex=exchange_name, q=queue_name, sep=cls.sep)
            if entry_name not in cls.queues:
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
                    'callbacks': [cls.__message_handler_builder(fun)],
                    'queue_name': queue_name,
                    'exchange_name': exchange_name,
                    'binding_key': binding_key,
                    'consumer_id': cls.count,
                    'task_name': fun.__qualname__,
                    'accept': ['json']
                }
            else:
                exit(
                    'Error - For function "{name}" - {msg} {sub_msg}'.format(
                        msg='A task already exists for this configuration',
                        sub_msg='(ex: {ex_name}, q: {q_name}, binding_key: {bk})'.format(
                            ex_name=exchange_name,
                            q_name=queue_name,
                            bk=binding_key,
                        ),
                        name=fun.__qualname__,
                    ))

            cls.count += 1

            return fun

        return _sub_fun

    @classmethod
    def build(cls, app):
        """Build consumer from decorated tasks"""

        def get_consumers(self, channel):
            return cls.__consumer_builder(channel)

        consumer_step = type(
            'MultiConsumer',
            (bootsteps.ConsumerStep,),
            {
                'get_consumers': get_consumers,
            }
        )

        cls._hello_message()

        app.steps['consumer'].add(consumer_step)

    @classmethod
    def _hello_message(cls):

        hello_message = '''---------------
---\033[92;1m*********\033[0m--- .> Skinos: {version}
--\033[92;1m***********\033[0m-- 
-\033[92;1m****\033[0m-----\033[92;1m****\033[0m- 
-\033[92;1m****\033[0m-----\033[92;1m****\033[0m- 
---\033[92;1m****\033[0m-------- .> with sentry: {sentry}
-----\033[92;1m****\033[0m------ .> number of exahnge(s): {n_ex}
------\033[92;1m*****\033[0m---- .> number of queues(s): {n_q}
--------\033[92;1m****\033[0m---
-\033[92;1m****\033[0m-----\033[92;1m****\033[0m-
-\033[92;1m****\033[0m-----\033[92;1m****\033[0m-
--\033[92;1m***********\033[0m--
---\033[92;1m*********\033[0m---
---------------'''.format(
    version='0.0.1',
    sentry=cls._with_sentry,
    n_ex=len(cls.exchanges),
    n_q=len(cls.queues)
        )
        print(hello_message)

        print('\033[92;1m[exchanges]')
        for _, exchange in cls.exchanges.items():
            print(' .> {ex_name}'.format(ex_name=str(exchange).replace('Exchange ', '')))
        print()

        print('[queues]')
        for _, queue in cls.queues.items():
            print(' .> {q_name} (ex:{ex_name}, b_key: {bk})'.format(
                q_name=queue.name,
                ex_name=str(queue.exchange).replace('Exchange ', ''),
                bk=queue.routing_key
            ))
        print()

        print('[tasks]')
        for _, meta_consumer in cls.meta_consumers.items():
            print(' .> {t_name} (ex: {ex_name}, q: {q_name}, b_key: {bk})'.format(
                t_name=meta_consumer['task_name'],
                ex_name=meta_consumer['exchange_name'],
                q_name=meta_consumer['queue_name'],
                bk=meta_consumer['binding_key']
            ))
        print('\033[0m')

    @classmethod
    def __message_handler_builder(cls, function):
        # Checsarguments count
        if function.__code__.co_argcount < 2:
            exit(
                'Error - In function "{name}" - too few arguments (except 2, received: {n})'.format(
                    name=function.__qualname__,
                    n=function.__code__.co_argcount
                ))

        def wrapper(body, msg):
            result = function(body, msg)
            msg.ack()
            return result

        wrapper.__name__ = 'wrapper_{}'.format(str(uuid.uuid4()).replace('-', ''))
        logger.debug('new message handler {}'.format(wrapper.__name__))
        return wrapper

    @classmethod
    def __consumer_builder(cls, channel):
        consumers = []
        for _, meta_consumer in cls.meta_consumers.items():
            queue_key = '{ex}{sep}{q}'.format(
                ex=meta_consumer['exchange_name'],
                q=meta_consumer['queue_name'],
                sep=cls.sep
            )
            consumers.append(Consumer(
                channel=channel,
                queues=[cls.queues[queue_key]],
                callbacks=meta_consumer['callbacks'],
                accept=meta_consumer['accept']
            ))
            logger.debug(
                'Consumer created (exchange: {ex}, queue: {q}, binding_key: {bk})'.format(
                    ex=meta_consumer['exchange_name'],
                    q=meta_consumer['queue_name'],
                    bk=meta_consumer['binding_key'],
                )
            )

        return consumers
