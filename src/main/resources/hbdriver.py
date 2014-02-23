import vertx # this must be at the top of the file.

from com.google.common.collect import Lists
from com.twitter.hbc import ClientBuilder
from com.twitter.hbc.core import Constants
from com.twitter.hbc.core.endpoint import StatusesFilterEndpoint, Location
from com.twitter.hbc.core.processor import StringDelimitedProcessor
from com.twitter.hbc.httpclient.auth import Authentication, OAuth1
from core.event_bus import EventBus
from core.shared_data import SharedData
from java.util.concurrent import LinkedBlockingQueue
from org.vertx.java.core.json import JsonObject
import functools
from credmgr import CredentialManager
from controller import start_server 

logger = vertx.logger()


class EventBusQueue(LinkedBlockingQueue):
    """Mocks the interface and instead publishes to the event bus."""
    def __init__(self, channel):
        self.channel = channel
        
    def offer(self, item, *args):
        EventBus.publish(self.channel, JsonObject(item))


class HBCAgent(object):
    STATUS_ADDRESS = "hbdriver:stati"
    SHUTDOWN_ADDRESS = "hbdriver:shutdown"
    
    def __init__(self, cfg, creds):
        self.id = cfg['id']
        self.cfg = cfg
        self.client = self._build_client(cfg, creds)
        self.address = "hbdriver:client:" + self.id
        self._handlers = []
        for addr, h in [(self.address, self.handler),
                        (self.STATUS_ADDRESS, self.status_handler),
                        (self.SHUTDOWN_ADDRESS, self.shutdown_handler)]:
            hid = EventBus.register_handler(addr, handler=h)
            self._handlers.append(hid)
            
    @property
    def name(self):
        if self.cfg.get('name'):
            return "%s-%s" % (self.cfg['name'], self.id)
        return self.id
            
    def start(self):
        logger.info("Connecting hosebird for %s..." % self.name)
        self.client.connect()
            
    def _build_client(self, cfg, creds):
        logger.info("Configuring hosebird for %s" % self.id)
        queue = EventBusQueue(cfg['publishChannel'])
        endpoint = StatusesFilterEndpoint()
        if cfg.get('track'):
            endpoint.trackTerms(cfg['track'])
        if cfg.get('follow'):
            endpoint.followings(cfg['follow'])
        if cfg.get('locations'):
            locations = [Location(Location.Coordinate(w, s), Location.Coordinate(e, n)) 
                         for s, w, n, e in cfg['locations']]
            endpoint.locations(locations)
        auth = OAuth1(*creds)
    
        client = ClientBuilder()\
          .hosts(Constants.STREAM_HOST)\
          .endpoint(endpoint)\
          .authentication(auth)\
          .processor(StringDelimitedProcessor(queue))\
          .build()
        return client

        
    def _status(self):
        t = self.client.getStatsTracker()
        stats = {}
        for a, v in [('getNum200s', '200s'),
                  ('getNum400s', '400s'),
                  ('getNumDisconnects', 'disconnects'),
                  ('getNumConnects', 'connects'),
                  ('getNumConnectionFailures', 'connfailures'),
                  ('getNumClientEventsDropped', 'eventsdropped')]:
            stats[v] = getattr(t, a)()

        if not self.client.isDone():
            return dict(status=200, address=self.address, stats=stats)
        else:
            return dict(status=500, address=self.address, status=stats)
        
    def status_handler(self, msg):
        EventBus.publish(msg.body['replyTo'], self._status())
        
    def shutdown_handler(self, msg):
        EventBus.publish(msg.body['replyTo'], self._shutdown())
    
    def _shutdown(self):
        logger.warn("Shutting down %s..." % self.address)
        EventBus.send(CredentialManager.ADDRESS, dict(command="release",
                                                      id=self.id))
        status = 200
        msg = None
        try:
            self.client.stop(100)
        except Exception, e:
            logger.error("Failed to stop: %s" % self.address, e)
            status = 500
            msg = str(e)
        self.client = None
        for hid in self._handlers:
            EventBus.unregister_handler(hid)
        logger.info("Shutdown %s: status=%s" % (self.address, status))
        return dict(status=status, address=self.address, msg=msg)

    def handler(self, msg):
        command = msg.body.get('command')
        if command == 'status':
            msg.reply(self._status())
            return
        if command in ('stop', 'shutdown'):
            msg.reply(self._shutdown())
            return
        msg.reply(dict(status=500, msg="Unknown command"))

    
def start_stream(msg):    
    cfg = msg.body
    
    def _on_credentials(cmsg):
        if cmsg.body['status'] == 200:
            client = HBCAgent(cfg, cmsg.body['credentials'])
            client.start()
            msg.reply(dict(status=200, clientAddress=client.address))
        else:
            msg.reply(dict(status=500, msg="Failure to acquire credentials", 
                           cause=cmsg.body))
        
    EventBus.send(CredentialManager.ADDRESS,
                  dict(command="acquire", id=cfg['id']),
                  _on_credentials)            


EventBus.register_handler('hbdriver:start', handler=start_stream)


def init_controller(config):
    if config.get('webserver') is not None:
        start_server(config['webserver'])


def init_credmgr(config):
    mgr = CredentialManager([])    
    for cfg in config.get("credentials") or []:
        mgr.add(cfg)
    key = config.get("credentialsEnvVar")
    if not key:
        return
    try:
        mgr.from_env(key)
    except KeyError, e:
        if mgr.size() == 0:
            logger.error("*" * 80)
            logger.error("No credentials found in the config, and credentials could not be loaded from env")
            logger.error("*" * 80)
            vertx.exit()
        

def init_autostart(config):
    for i, cfg in enumerate(config.get('autostart', [])):
        if not cfg.get('enabled', True):
            logger.info("Skipping disabled config #%d" % i)
            continue
        cfg['id'] = "autostart-" + str(i)
        EventBus.send('hbdriver:start', cfg, lambda msg: logger.info("Started hosebird for config #%d: %s" % (i, msg.body)))


def init_test_setup(config):
    if not config.get('testmode', False):
        return 

    def register(addr):
        def _wrapper(func):
            EventBus.register_handler(addr, handler=func)
            return func
        return _wrapper

    @register('test.tweets')
    def tweet_handler(msg):
        print msg.body

    @functools.partial(vertx.set_periodic, 1000)
    def status(tid):
        EventBus.publish(HBCAgent.STATUS_ADDRESS, dict(replyTo="test.status"))

    @register('test.status')
    def print_status(msg):
        print "Status >>>", msg.body

    @register('test.shutdown')
    def on_shutdown(msg):
        print "Shutdown >>>", msg.body
        vertx.exit()
        
    # simple shutdown hook
    exit_after = config.get('testmode').get('exitAfter', 10)
    if exit_after:
        vertx.set_timer(exit_after * 1000, 
                        lambda x: EventBus.publish(HBCAgent.SHUTDOWN_ADDRESS, 
                                                   dict(replyTo="test.shutdown")))



    
    