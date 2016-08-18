import logging
import numbers

try:
    import redis
except ImportError:
    redis = None

try:
    from urllib.parse import urlparse, urljoin, quote, unquote
except ImportError:
    from urlparse import urlparse, urljoin
    from urllib import quote, unquote


DEFAULT_REDIS_PRIORITY_STEPS = [0, 3, 6, 9]

logger = logging.getLogger(__name__)


class BrokerBase(object):
    def __init__(self, broker_url, *args, **kwargs):
        purl = urlparse(broker_url)
        self.host = purl.hostname
        self.port = purl.port
        self.vhost = purl.path[1:]

        username = purl.username
        password = purl.password

        self.username = unquote(username) if username else username
        self.password = unquote(password) if password else password


class Redis(BrokerBase):
    sep = '\x06\x16'
    last_values = {}

    def __init__(self, broker_url, *args, **kwargs):
        super(Redis, self).__init__(broker_url)
        self.host = self.host or 'localhost'
        self.port = self.port or 6379
        self.vhost = self._prepare_virtual_host(self.vhost)

        if not redis:
            raise ImportError('redis library is required')

        self.redis = redis.Redis(
            host=self.host,
            port=self.port,
            db=self.vhost,
            password=self.password,
        )

        broker_options = kwargs.get('broker_options')

        if broker_options and 'priority_steps' in broker_options:
            self.priority_steps = broker_options['priority_steps']
        else:
            self.priority_steps = DEFAULT_REDIS_PRIORITY_STEPS

    def _q_for_pri(self, queue, pri):
        if pri not in self.priority_steps:
            raise ValueError('Priority not in priority steps')
        return '{0}{1}{2}'.format(*((queue, self.sep, pri) if pri else (queue, '', '')))

    def get_queues(self):
        return self.redis.keys()

    def itercounts(self):
        queues = self.get_queues()

        count = lambda name: sum([
            self.redis.llen(x)
            for x in [self._q_for_pri(name, pri) for pri in self.priority_steps]
        ])

        submitted = set()
        for name in queues:
            try:
                value = count(name)
            except:
                if name not in self.last_values:  # Skip unknown empty queues
                    continue
                else:
                    value = 0

            if 'reply.celery.pidbox' in name:
                continue

            yield name, value
            self.last_values.update({name: value})

    def _prepare_virtual_host(self, vhost):
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = 0
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError('Database is int between 0 and limit - 1, not {0}'.format(vhost))
        return vhost
