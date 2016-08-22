import logging
import signal

from Queue import Empty
from multiprocessing.queues import Queue

from backend.influx import TaskStats, QueueStats
from queue import CeleryQueue


logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)


def process(itm):
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
    for name, count in itm['queues'].iteritems():
        QueueStats(queue=name, count=count)
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
