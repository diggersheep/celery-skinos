# Celery Skinos


Custom consumer for celery integration.

## Usage

```PYTHON
from skinos.custom_consumer import CustomConsumer
```

### Define a new exchange

defined a new exchange with a name and a binding key (always a topic).
The exchange name must be unique.

```PYTHON
# add_exchange(str, str) -> Exchange
CustomConsumer.add_exchange('test', "test.*.*")
```


### Define a new task

Define a new message handler 

decoration take 3 arguments:

- exchange name (must be defined)
- queue name (must be defined)
- queue binding key


Function but have this prototype: `(str, Message) -> Any`
- `body` is the payload 
- `msg` is the message object (kombu.transport.myamqp.Message)


```PYTHON
# consumer(str, str, str) -> Callable[[str, Message], Any]
@CustomConsumer.consumer('test', 'test.test', 'test.test.*')
def coucou(body, msg):
    print('payload content : {}'.format(body))
    print('message object content : {}'.format(msg))
```

### Build consumers for Celery integration

Build consumers itself. all previous methods are just a pre-configuration for this build.
It take one argument, which is the Celery app.
```PYTHON
# build(Celery) -> None
CustomConsumer.build(app)
```

### Add Sentry handler

You must init Sentry normally for a Celery project.
Then Skinos is able to catch exception and send it sentry.

set sentry to True and set raise to False (i.e: if error occur, error is not re-raise, but ignored)
if you don't use it, default values are False and False

```
# with_sentry(bool, bool) -> Tuple(bool, bool)
CustomConsumer.with_sentry(False, False)
```

### Run celery

Run celery normally



