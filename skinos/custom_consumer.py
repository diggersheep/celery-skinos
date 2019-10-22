"""Skinos"""

from typing import Dict, Any, Callable, Type, List, Tuple
import logging as logger

import uuid
from kombu import Exchange, Queue, Consumer
from kombu.transport.pyamqp import Message
from celery import bootsteps
import sentry_sdk as sentry

from skinos import VERSION


class CustomConsumer:
    """Class for CustomConsumer"""
    exchanges: Dict[str, Exchange] = {}
    queues: Dict[str, Queue] = {}
    meta_consumers: Dict[str, Any] = {}
    count: int = 0
    sep: str = '|'
    _with_sentry: bool = False
    _sentry_raise: bool = True

    # format for meta_consumers
    # meta_consumers = {
    #     'exchange.queue': {
    #         'exchange_name': '',
    #         'queue_name': '',
    #         'callbacks': [one_function]
    #     }
    # }

    @classmethod
    def with_sentry(cls, __with_sentry: bool, _raise: bool = True) -> Tuple[bool, bool]:
        """
        set sentry
        :param __with_sentry:
        :param _raise:
        :return: with sentry boolean
        """
        if not isinstance(__with_sentry, bool):
            cls._with_sentry = False
            raise TypeError('Got type {}, expected bool.'.format(type(__with_sentry)))

        if not isinstance(_raise, bool):
            cls._sentry_raise = False
            raise TypeError('Got type {}, expected bool.'.format(type(_raise)))

        cls._with_sentry = __with_sentry
        cls._sentry_raise = _raise
        return __with_sentry, _raise

    @classmethod
    def add_exchange(cls, exchange_name: str, routing_key: str = '#') -> Exchange:
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
        else:
            raise RuntimeError('Topic already defined (ex: {ex}, b_key: {bk})'.format(
                ex=exchange_name,
                bk=routing_key
            ))
        return cls.exchanges[exchange_name]

    @classmethod
    def consumer(cls, exchange_name: str, queue_name: str, binding_key: str) -> Any:
        """
        :param app:
        :param exchange_name:
        :param queue_name:
        :param binding_key:
        :return:
        """

        def _sub_fun(fun: Callable[[str, Any], Any]) -> Callable[[str, Any], Any]:
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
                    'accept': ['json', 'text/plain']
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
    def build(cls, app: Any) -> None:
        """Build consumer from decorated tasks"""

        def get_consumers(self: Type[bootsteps.ConsumerStep], channel: Any) -> Any:
            return cls.__consumer_builder(channel)

        consumer_step = type(
            'CustomConsumer',
            (bootsteps.ConsumerStep,),
            {
                'get_consumers': get_consumers,
            }
        )

        cls._hello_message()

        app.steps['consumer'].add(consumer_step)

    @classmethod
    def _hello_message(cls) -> None:
        # Skinos MOTD
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
    version=VERSION,
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
    def __message_handler_builder(cls, function: Callable[[str, Type[Message]], Any]) -> Any:
        """
        decorator for message handler
        :param function:
        :return:
        """
        # Check arguments count
        if function.__code__.co_argcount < 2:
            exit(
                'Error - In function "{name}" - too few arguments (except 2, received: {n})'.format(
                    name=function.__qualname__,
                    n=function.__code__.co_argcount
                ))

        def wrapper(body: str, msg: Type[Message]) -> Any:
            """
            wrapper
            :param body: str
            :param msg: komby.transport.pyamqp.Message
            :return:
            """
            result = None
            try:
                result = function(body, msg)
            except Exception as ex:
                logger.error(str(ex))
                if cls._with_sentry:
                    sentry.capture_exception(ex)
                if cls._sentry_raise:
                    raise

            # ACK if it's not a bool or is result is set to True
            if not isinstance(result, bool):
                msg.ack()
            elif isinstance(result, bool) and result:
                msg.ack()

            return result

        wrapper.__name__ = 'wrapper_{}'.format(str(uuid.uuid4()).replace('-', ''))
        logger.debug('new message handler {}'.format(wrapper.__name__))
        return wrapper

    @classmethod
    def __consumer_builder(cls, channel: Any) -> List[Consumer]:
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
