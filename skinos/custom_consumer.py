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

    @classmethod
    def consumer(cls, app,  exchange_name, queue_name, binding_key):
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
