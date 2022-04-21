"""
Celery worker base

for test: celery -A demo.celery worker -l info
"""

from celery import Celery
import sentry_sdk as sentry

# sentry integration for classic tasks
from sentry_sdk.integrations.celery import CeleryIntegration

import logging
logging.basicConfig(level=logging.INFO)

# custom customer class
from skinos.custom_consumer import CustomConsumer


CELERY_BROKER = "amqp://{user}:{password}@{host}:{port}{vhost}".format(
    user='guest',
    password='guest',
    host='127.0.0.1',
    port='5672',
    vhost='/'
)
BROKER_TRANSPORT_OPTIONS = {'confirm_public': True}

# Celery init
app = Celery(
    'test',
    backend='',
    broker=CELERY_BROKER,
    include=[]
)
app.config_from_object(BROKER_TRANSPORT_OPTIONS)

# Init sentry
# sentry.init(
#     "my_projet_key",
#     integrations=[CeleryIntegration()]
# )

############################################
#################  SKINOS  #################
############################################

# set sentry to True and set raise to False (i.e: if error occur, error is not re-raise, but ignored)
# if you don't use it, default values are False and False
CustomConsumer.with_sentry(False, False)

# defined a new exchange with a name and a binding key (always a topic)
CustomConsumer.add_exchange('test', "test.*.*")


# Define a new message handler
# decoration take 3 arguments:
#   - exchange name (must be defined)
#   - queue name (must be defined)
#   - queue binding key
# Function but have this prototype: (str, Messahe) -> Any
#   - body is the payload
#   - msg is the message object (kombu.transport.myamqp.Message)
#
# /!\ only one task per queue
@CustomConsumer.consumer('test', 'test.test', 'test.test.*')
def hello(body, msg):
    print('COUCOU')


@CustomConsumer.consumer('test', 'test.hello', 'test.hello.*')
def world(body, msg):
    raise RuntimeError('Je viens de planter :(')


# build consumers
# take 1 parameter
#   - app is the celery app
CustomConsumer.build(app)
