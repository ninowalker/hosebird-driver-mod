import vertx # this must be at the top of the file.
from config import ClientConfig
from core.event_bus import EventBus
from juice import Juice
from org.apache.commons.collections.buffer import CircularFifoBuffer
from pipeline import HosebirdSpout, RecentSampler
import time


logger = vertx.logger()


class PluralActor(object):
    """
    Class the facilitate communication between one or all instances.
    """

    def handler(self, msg):
        """
        Given a directed to a single actor, command, use introspection to
        invoke the command an send a reply.
        """
        command = msg.body['command']
        logger.debug("%s-%s.rpc(%s)" % (self.__class__.__name__, id(self), command))
        reply = getattr(self, "handle_%s" % command)(msg)
        reply['command'] = command
        reply['from'] = self.address
        msg.reply(reply)

    def multicast_handler(self, msg):
        command = msg.body['command']
        replyTo = msg.body.get('replyTo')
        logger.debug("%s-%s.multicast(%s) -> %s" % (self.__class__.__name__, id(self),
                                                    command, replyTo))
        reply = getattr(self, "handle_%s" % command)(msg)
        reply['command'] = command
        reply['from'] = self._address
        if replyTo:
            EventBus.publish(replyTo, reply)

    def bind(self, bus, address, mcast_address):
        self._address = address
        self._handlers = []
        for addr, h in [(address, self.handler),
                        (mcast_address, self.multicast_handler)]:
            self._handlers.append(bus.register_handler(addr, handler=h))

    def unbind(self, bus):
        for hid in self._handlers:
            bus.unregister_handler(hid)


class HosebirdDriver(PluralActor):
    MULTICAST = "hbdriver.multicast"
    ANNOUNCE_ADDRESS = "hbdriver.announce"

    def __init__(self, config):
        self.id = config['id']
        self.address = config['instanceAddress']
        self.cfg = config
        self.pipeline = Juice.pipeline_factory(config)
        self.bind(EventBus, self.address, self.MULTICAST)

    @property
    def name(self):
        return self.id

    def announce(self, m):
        m['name'] = self.name
        m['address'] = self.address
        EventBus.publish(self.ANNOUNCE_ADDRESS, m)

    def start(self):
        logger.info("Starting pipeline for %s..." % self.name)
        self.pipeline.start()
        self.announce(dict(action="started"))

    def handle_status(self, _):
        return dict(status=200, address=self.address,
                    stats=self.pipeline.stats())

    def handle_cycle(self, _):
        self.pipeline.get(HosebirdSpout).reset()
        self.announce(dict(action="cycled"))
        return dict(status=200)

    def handle_last_n(self, msg):
        n = msg.body['n']
        recent = self.pipeline.get(RecentSampler)
        if not recent:
            return dict(status=404)
        return recent.recent(n)

    def handle_get_config(self, _):
        return dict(config=self.cfg, status=200)

    def handle_shutdown(self, _):
        logger.warn("Shutting down %s..." % self.address)
        self.pipeline.shutdown()
        logger.info("Shutdown %s" % self.address)
        self.announce(dict(action="shutdown"))
        return dict(status=200, address=self.address)

    @classmethod
    def handle_new(cls, msg):
        cfg = msg.body['config']
        driver = HosebirdDriver(ClientConfig(cfg))
        driver.start()
        return dict(status=200, address=driver.address)

EventBus.register_handler('hbdriver.new', handler=HosebirdDriver.handle_new)


class Secretary(PluralActor):
    """
    Responsible for tracking the comings and goings and important announcements.

    While not a plural actor in a single verticle, it's plural across multiple
    verticles and actors.
    """

    MULTICAST = 'hbdriver.secretaries'

    def __init__(self):
        self.recent = CircularFifoBuffer(200)
        self.id = id(self)
        self.address = 'hbsecretary.%s' % self.id

        self.bind(EventBus, self.address, self.MULTICAST)

        EventBus.register_handler(self.address, handler=self.handler)
        EventBus.register_handler(HosebirdDriver.ANNOUNCE_ADDRESS,
                                  handler=self.handle_announcement)
        # announce our presence for auditing
        EventBus.publish(HosebirdDriver.ANNOUNCE_ADDRESS,
                         dict(action="secretary.init", id=self.id))
        # ask everyone to report status to read the health of the cluster
        EventBus.publish(HosebirdDriver.MULTICAST,
                         dict(command="status",
                              replyTo=HosebirdDriver.ANNOUNCE_ADDRESS))

    def handle_announcement(self, msg):
        self.recent.add(dict(time=time.time(), m=msg.body))

    def handle_recent_history(self, msg):
        n = msg.body.get('n', 20)
        return dict(status=200, history=list(self.recent)[0:n])




# ultimately this should be cluster safe
# and use shared data
class CredentialManager(object):
    def __init__(self):
        self._pool = []
        self._inuse = {}

    def size(self):
        return len(self._pool) + len(self._inuse)

    def add(self, creds):
        if isinstance(creds, dict):
            creds = creds['consumerKey'], creds['consumerSecret'], creds['appToken'], creds['appSecret']
        self._pool.append(creds)

    def lease(self, for_whom):
        try:
            creds = self._pool.pop(0)
            self._inuse[for_whom] = creds
            logger.info("%s has acquired credentials" % for_whom)
            return creds
        except IndexError:
            raise Exception("No credentials available")

    def release(self, from_whom):
        creds = self._inuse.pop(from_whom)
        self._pool.append(creds)

    @classmethod
    def from_env(self, key):
        try:
            creds = vertx.env()[key]
            consumer_key, consumer_secret, app_token, app_secret = creds.split(" ")
            return [consumer_key, consumer_secret, app_token, app_secret]
        except Exception, e:
            logger.warn("Credentials could not be loaded from env var: %s: %s" % (key, e))
            return None

    @classmethod
    def init(cls, cfg):
        mgr = cls()
        for c in cfg.get("credentials") or []:
            mgr.add(c)
        key = cfg.get("credentialsEnvVar")
        if key:
            try:
                mgr.from_env(key)
            except KeyError:
                pass
        if mgr.size() == 0:
            logger.error("*" * 80)
            logger.error("No credentials found in the config, and credentials could not be loaded from env")
            logger.error("*" * 80)
            vertx.exit()
        return mgr


class WebController(object):
    # Configuration for the web server
    web_server_conf = {
        'module': 'io.vertx~mod-web-server~2.0.0-final',

        'port': 8080,
        'host': 'localhost',
        'ssl': False,
        'bridge': True,

        'inbound_permitted': [
            dict(address='hbdriver'),
            dict(address='hbdriver.web_controller'),
            # this could be tighter, if you're paranoid
            dict(address='hbdriver.multicast'),
            dict(address_re=r'hbinstance\..*\.trace'),
            dict(address_re=r'hbinstance\..*'),
        ],

        'outbound_permitted': [
            dict(address_re=r'hbdriver\.browser\..+'),
        ]
    }

    def __init__(self, cfg):
        m = dict(self.web_server_conf)
        m.update(cfg)
        # Start the web server, with the config we defined above
        module = m.pop('module')
        vertx.deploy_module(module, m)
        #EventBus.register_handler('hbdriver.web_controller',
        #                          handler=self.handler)
