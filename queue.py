import datetime
import json
import logging
import os
import time
from collections import defaultdict
from multiprocessing import Event, Process
from threading import Thread

from celery import Celery
from celery.app.control import Control
from celery.events import EventReceiver
from celery.events.snapshot import Polaroid
from celery.events.state import State

from broker import Redis

logger = logging.getLogger(__name__)


def reavg(old, count, delta):
    return (old * count + delta) / (count + 1)


class CeleryConfig(object):
    BROKER_URL = os.environ.get('CELERY_BROKER_URL')
    CELERY_ENABLE_UTC = True


class CeleryConsumer(Thread):
    def __init__(self, state, celery):
        super(CeleryConsumer, self).__init__()
        self.celery = celery
        self.state = state

    def run(self):
        with self.celery.connection() as conn:
            self.receiver = EventReceiver(
                conn,
                handlers={'*': self.state.event},
                app=self.celery,
            )
            self.receiver.capture()


class CeleryRecorder(Polaroid):
    clear_after = True
    redis = Redis(CeleryConfig.BROKER_URL)

    def __init__(self, queue, *args, **kwargs):
        self.queue = queue
        super(CeleryRecorder, self).__init__(*args, **kwargs)

    def on_shutter(self, state):
        try:
            self.gather_data(state)
        except Exception as ex:
            logger.exception(str(ex))

    def gather_data(self, state):
        now = datetime.datetime.utcnow()

        counts = defaultdict(  # Worker
            lambda: defaultdict(  # Task name
                lambda: defaultdict(  # Task state
                    lambda: dict({
                        'count': 0,
                        'avg_wait': 0.,
                        'max_wait': 0.,
                        'avg_exec': 0.,
                        'max_exec': 0.,
                    })
                )
            )
        )

        tasks = 0

        # Gather stats
        for uuid, task in self.state.tasks_by_time():
            tasks += 1

            worker = task.worker.hostname if task.worker else ''
            if worker and '@' in worker:
                worker = worker.split('@', 1)[1]  # celery@<hostname> -> <hostname>

            obj = counts[worker][task.name][task.state]
            obj['count'] += 1

            if task.eta is None and task.started and task.received and task.succeeded:
                wait = task.started - task.received
                exc = task.succeeded - task.started
                obj['avg_wait'] = reavg(obj['avg_wait'], obj['count'] - 1, wait)
                if wait > obj['max_wait']:
                    obj['max_wait'] = wait

                obj['avg_exec'] = reavg(obj['avg_exec'], obj['count'] - 1, exc)
                if exc > obj['max_exec']:
                    obj['max_exec'] = exc

        logger.debug("Gathered %s celery tasks", tasks)

        self.queue.put({
            'now': now,
            'tasks': json.loads(json.dumps(counts)),
            'queues': dict(self.redis.itercounts()),
        })


class CeleryQueue(Process):
    _running = Event()

    def __init__(self, queue, *args, **kwargs):
        self.queue = queue
        super(CeleryQueue, self).__init__(*args, **kwargs)

    def run(self):
        self.celery = Celery()
        self.celery.config_from_object(CeleryConfig)
        self.control = Control(self.celery)
        self.enable_event = datetime.datetime(2010, 1, 1)

        self.state = State()
        self.last_query = datetime.datetime.utcnow()

        self.consumer = CeleryConsumer(self.state, self.celery)
        self.consumer.start()

        logger.info("Celery monitor started")

        freq = float(os.environ.get('FREQUENCY', 10))
        with CeleryRecorder(self.queue, self.state, freq=freq):
            while not self._running.is_set():
                time.sleep(.1)

                # Periodically re-enable celery control events
                if (datetime.datetime.utcnow() - self.enable_event).total_seconds > 600:
                    self.control.enable_events()
                    self.enable_events = datetime.datetime.utcnow()

        logger.info('Celery monitor stopped')

    def stop(self):
        self._running.set()
