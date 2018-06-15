from kombu import Exchange, Queue, Consumer
from kombu.exceptions import DecodeError
from celery import bootsteps
from sys import stderr


class FailureConsumer:
    def __init__(self):
        self.exchange = Exchange(
            'failure',
            type='topic',
            routing_key='failure.#'
        )
        self.queues = {
            'reject': Queue('failure.reject', self.exchange, 'failure.reject'),
            'retry': Queue('failure.retry', self.exchange, 'failure.retry'),
            'error': Queue('failure.error', self.exchange, 'failure.error')
        }
        self.consumers = {
            'reject': None,
            'retry': None,
            'error': None
        }

    def __check_unique(self, q):
        if self.consumers[q] is not None:
            exit("Only on consumer is allowed for the 'failure.{q}' queue".format(q=q))

    def reject(self, app):
        # Check if reject consumer is already define (only on is allowed per worker)
        self.__check_unique('reject')

        def deco(fun):

            queue = self.queues['reject']

            def get_consumers(self, ch):
                return [Consumer(ch, queues=[queue], callbacks=[self.handle_message], accept=['json'])]

            def handle_message(self, body, message):
                result = fun(body, message)
                # anyway consume the message (no ack = message stayed in queue = bad !)
                message.ack()
                return result

            self.consumers['reject'] = type('reject', (bootsteps.ConsumerStep,), {'get_consumers': get_consumers, 'handle_message': handle_message})
            app.steps['consumer'].add(self.consumers['reject'])

            return fun

        return deco



    def error(self, app):
        # Check if reject consumer is already define (only on is allowed per worker)
        self.__check_unique('error')

        def deco(fun):

            queue = self.queues['error']

            def get_consumers(self, ch):
                return [Consumer(ch, queues=[queue], callbacks=[self.handle_message], accept=['json'])]

            def handle_message(self, body, message):
                result = fun(body, message)
                # anyway consume the message (no ack = message stayed in queue = bad !)
                message.ack()
                return result

            self.consumers['reject'] = type('error', (bootsteps.ConsumerStep,), {'get_consumers': get_consumers, 'handle_message': handle_message})
            app.steps['consumer'].add(self.consumers['error'])

            return fun

        return deco

    def retry(self, app):
        # Check if reject consumer is already define (only on is allowed per worker)
        self.__check_unique('retry')

        def deco(fun):

            queue = self.queues['retry']

            def get_consumers(self, ch):
                return [Consumer(ch, queues=[queue], callbacks=[self.handle_message], accept=['json'])]

            def handle_message(self, body, message):
                result = fun(body, message)
                # anyway consume the message (no ack = message stayed in queue = bad !)
                message.ack()
                return result

            self.consumers['retry'] = type('retry', (bootsteps.ConsumerStep,), {'get_consumers': get_consumers, 'handle_message': handle_message})
            app.steps['consumer'].add(self.consumers['retry'])

            return fun

        return deco
