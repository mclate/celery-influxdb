import os
import logging
import signal

from Queue import Empty
from multiprocessing.queues import Queue

from backend.influx import TaskStats, QueueStats
from queue import CeleryQueue


debug = os.environ.get('DEBUG', False) in ['true', 'yes', '1', 'True', 'Yes', 'Y', 'y']
logging.basicConfig(level=logging.INFO if not debug else logging.DEBUG)

log = logging.getLogger(__name__)


def process(itm):
    count = 0
    for worker, tasks in itm['tasks'].iteritems():
        for name, states in tasks.iteritems():
            for state, counts in states.iteritems():
                TaskStats(
                    name=name,
                    worker=worker, 
                    state=state,
                    count=counts['count'],
                    avg_exec_in_millis=counts['avg_exec'],
                    max_exec_in_millis=counts['max_exec'],
                    avg_wait_in_millis=counts['avg_wait'],
                    max_wait_in_millis=counts['max_wait'],
                )
                count += 1
    log.debug('Gathered %s events', count)

    for name, count in itm['queues'].iteritems():
        QueueStats(queue=name, count=count)
        log.debug('Queue %s: %s', name, count)

    QueueStats.commit()


def main():
    queue = Queue()

    celery_queue = CeleryQueue(queue)

    def stop(*_):
        celery_queue.stop()
        celery_queue.join()
        TaskStats.commit()
        QueueStats.commit()


    try:
        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)
        celery_queue.start()

        while True:
            try:
                itm = queue.get(True, 1)
                process(itm)
            except Empty:
                pass
            except Exception as ex:
                log.exception(str(ex))

    except KeyboardInterrupt:
        stop()



if __name__ == '__main__':
    main()
