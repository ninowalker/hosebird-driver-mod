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

config = vertx.config()
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
    
    def __init__(self, client, key):
        self.client = client
        self.address = "hbdriver:client:" + key
        self._handlers = []
        for addr, h in [(self.address, self.handler),
                        (self.STATUS_ADDRESS, self.status_handler),
                        (self.SHUTDOWN_ADDRESS, self.shutdown_handler)]:
            hid = EventBus.register_handler(addr, handler=h)
            self._handlers.append(hid)
        
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
        status = 200
        msg = None
        try:
            self.client.stop(0)
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
    
def start_stream(msgw):
    msg = msgw.body
    
    key = msg['consumerKey']
    logger.error("Checking if %s is already in use..." % key)
    clients = {} #SharedData.get_hash('hbdriver.clients')
    if key in clients:
        logger.error("Client already started.")
        msgw.reply(dict(status=409))
        return
    logger.info("Configuring hosebird for %s" % key)
    queue = EventBusQueue(msg['publishChannel'])
    endpoint = StatusesFilterEndpoint()
    if msg.get('track'):
        endpoint.trackTerms(msg['track'])
    if msg.get('follow'):
        endpoint.followings(msg['follow'])
    if msg.get('locations'):
        locations = [Location(Location.Coordinate(w, s), Location.Coordinate(e, n)) 
                     for s, w, n, e in msg['locations']]
        endpoint.locations(locations)
    auth = OAuth1(msg['consumerKey'], msg['consumerSecret'], msg['appToken'], msg['appSecret'])
    
    client = ClientBuilder()\
      .hosts(Constants.STREAM_HOST)\
      .endpoint(endpoint)\
      .authentication(auth)\
      .processor(StringDelimitedProcessor(queue))\
      .build()

    logger.info("Connecting hosebird for %s..." % key)
    client.connect()
    
    clientw = HBCAgent(client, key)            
    msgw.reply(dict(status=200, clientAddress=clientw.address))


EventBus.register_handler('hbdriver:start', handler=start_stream)


test_mode = config.get('testmode', False)
if test_mode:
    try:
        creds = vertx.env()['OAUTH_CREDENTIALS']
        consumer_key, consumer_secret, app_token, app_secret = creds.split(" ")
        for i, cfg in enumerate(config.get('autostart', [])):
            cfg['consumerKey'] = consumer_key
            cfg['consumerSecret'] = consumer_secret
            cfg['appToken'] = app_token
            cfg['appSecret'] = app_secret
    except Exception, e:
        logger.error("*" * 80)
        logger.error("Credentials could not be loaded fron environment var: OAUTH_CREDENTIALS: %s" % e)
        logger.error("*" * 80)
        vertx.exit()
    
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

    vertx.set_timer(10000, lambda x: EventBus.publish(HBCAgent.SHUTDOWN_ADDRESS, 
                                                      dict(replyTo="test.shutdown")))


for i, cfg in enumerate(config.get('autostart', [])):
    if not cfg.get('enabled', True):
        logger.info("Skipping disabled config #%d" % i)
        continue
    
    EventBus.send('hbdriver:start', cfg, lambda msg: logger.info("Started hose for config #%d: %s" % (i, msg.body)))

    
    