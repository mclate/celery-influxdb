import gevent.monkey

gevent.monkey.patch_all()

import logging
import os
from collections import defaultdict

from celery import Celery

from broker import Redis
from gevent import Greenlet
from influx import TaskStats, QueueStats, WorkerStats
from expiringdict import ExpiringDict

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

freq = float(os.environ.get('FREQUENCY', 10))
BROKER_URL = os.environ.get('CELERY_BROKER_URL')
celery = Celery(broker=BROKER_URL)
redis = Redis(BROKER_URL)


def reavg(old, count, delta):
    return (old * count + delta) / (count + 1)


tasks = defaultdict(
    lambda: dict(
        received=0,
        started=0,
        succeeded=0,
        retried=0,
        failed=0,
        revoked=0,
        rejected=0,
        avg_wait=0.,
        max_wait=0.,
        avg_exec=0.,
        max_exec=0.,
    )
)

heartbeats = set()

# We don't receive event when task is ignored or revoked, thus there will always be ghost uids in the system that
# never go away. We'll purge them after a week
tuids = ExpiringDict(max_len=10240, max_age_seconds=60 * 60 * 24 * 7)


def event_dispatcher(event):
    handlers = {
        'task-succeeded': task_handler,
        'task-sent': task_handler,
        'task-received': task_handler,
        'task-started': task_handler,
        'task-failed': task_handler,
        'task-rejected': task_handler,
        'task-revoked': task_handler,
        'task-retried': task_handler,
        'worker-heartbeat': worker_heartbeat,
        'worker-online': worker_heartbeat,
    }
    func = handlers.get(event['type'], None)
    if func:
        gevent.spawn(func, event)
    else:
        log.debug(f'Received event with no handler: {event["type"]}')


def worker_heartbeat(event):
    global heartbeats
    heartbeats.add(event['hostname'])


def task_handler(event):
    # log.info(event['type'])
    # log.info(event)
    # log.info('    ')
    # return
    try:
        cname = event['type'][5:]
        uuid = event['uuid']

        if cname == 'received':
            name = event['name']
            tuids.update({uuid: {'name': name, 'received': event['timestamp'], 'started': 0}})
        elif cname == 'started':
            if uuid not in tuids:
                return
            name = tuids[uuid]['name']
            tuids[uuid]['started'] = event['timestamp']

            waited = tuids[uuid]['started'] - tuids[uuid]['received']
            tasks[name]['avg_wait'] = reavg(tasks[name]['avg_wait'], tasks[name]['received'], waited)
            tasks[name]['max_wait'] = max(tasks[name]['max_wait'], waited)
        else:
            if uuid not in tuids:
                return
            name = tuids[uuid]['name']

        if not name:
            log.error(f'Event for task with no name: {event}')
            return

        tasks[name][cname] += 1
        if cname in ['succeeded', 'failed', 'retried', 'rejected', 'revoked']:

            if tuids[uuid]['started']:
                exec = event['timestamp'] - tuids[uuid]['started']
                tasks[name]['avg_exec'] = reavg(tasks[name]['avg_exec'], tasks[name]['received'], exec)
                tasks[name]['max_exec'] = max(tasks[name]['max_exec'], exec)
            del tuids[uuid]

        # log.info(f'{name} - {cname}')
        # log.info(tasks[name])
        # log.info(len(tuids))
    except Exception as ex:
        log.exception(str(ex), event)


class Collector(Greenlet):
    def _run(self):
        log.debug(f'Started collecting from {BROKER_URL}')
        try:
            with celery.connection() as connection:
                recv = celery.events.Receiver(
                    connection,
                    handlers={'*': event_dispatcher},
                )
                log.debug(f'Receiver: {recv}')
                recv.capture(limit=None, timeout=None, wakeup=True)
        except (KeyboardInterrupt, SystemExit):
            return


class Submitter(Greenlet):
    def _run(self):
        global heartbeats
        try:
            while True:
                gevent.sleep(freq)

                for name, value in tasks.items():
                    TaskStats(
                        task=name,
                        **value,
                    )

                for name, count in redis.itercounts():
                    log.info(f'Report queue: {name} = {count}')
                    QueueStats(queue=name, count=count)

                WorkerStats(count=len(heartbeats))
                log.info(f'Report {len(heartbeats)} workers')
                heartbeats = set()
        except (KeyboardInterrupt, SystemExit):
            return


if __name__ == '__main__':
    s = Submitter()
    s.start()

    g = Collector()
    g.start()

    try:
        gevent.joinall([s, g])
    except (KeyboardInterrupt, SystemExit):
        pass
