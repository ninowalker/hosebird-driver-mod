import vertx
from core.event_bus import EventBus
from java.lang import System
import os

logger = vertx.logger()

def start_server(cfg):
    m = {}
    m.update(web_server_conf)
    m.update(cfg)
    # Start the web server, with the config we defined above
    module = m.pop('module')
    vertx.deploy_module(module, m)
    BrowserController(cfg)

   
class BrowserController(object):
    def __init__(self, cfg):
        self.cfg = cfg
        EventBus.register_handler('hbdriver.bcontroller', handler=self)
        
    def __call__(self, msg):
        cmd = msg.body['command']
        getattr(self, cmd)(msg)
        
    def handle_subscribe(self, msg):
        type = msg.body['type']
        self.subscribers[type].add(msg.body['replyTo'])
        return dict(status=200, address=self.address)
    
    def handle_unsubscribe(self, msg):
        type = msg.body['type']
        self.subscribers[type].remove(msg.body['replyTo'])
        return dict(status=200, address=self.address)
        
    def stream_sniff(self, msg):
        publish_to = msg['publishTo']
        duration = msg.get('duration', 60)
        filter_ = msg['filter'].split(" ")
        
        name = 'sniffer.%s' % publish_to
        hid = EventBus.register_handler(name,
                                        functools.partial(self.relay, fillter_, publish_to))
        
        vertx.set_timer(duration * 60, functools.partial(self.stop_sniff, hid, msg))
        
        EventBus.publish("hbdriver.stati", dict(replyTo="hbdriver.controller", 
                                                correlationId=name))
        logger.info("Setup relay from %s to %s" % (name, publish_to))
        
    def relay(self, f, publish_to, msg):
        t = msg.body.get('text', '').lower()
        if not t: 
            return
        for _f in f:
            if _f not in t:
                return 
        EventBus.publish(msg['publishTo'], msg.body)
        
        
    def stop_sniff(self, hid, msg, *args):
        EventBus.unregister_handler(hid)
        EventBus.publish(msg['publishTo'], dict(shutdown=True))
        
        


# Configuration for the web server
web_server_conf = {
    'module': 'io.vertx~mod-web-server~2.0.0-final',

    # Normal web server stuff
    'port': 8080,
    'host': 'localhost',
    'ssl': False,

    # Configuration for the event bus client side bridge
    # This bridges messages from the client side to the server side event bus
    'bridge': True,

    # This defines which messages from the client we will let through
    # to the server side
    'inbound_permitted':  [
        {
            'address_re': r'hbdriver\..*'
        },
        # Allow calls to login
        
        {
            'address': 'vertx.basicauthmanager.login'
        },
        # Allow calls to get static album data from the persistor
        {
            'address': 'vertx.mongopersistor',
            'match': {
                'action': 'find',
                'collection': 'albums'
            }
        },
        # And to place orders
        {
            'address': 'vertx.mongopersistor',
            'requires_auth': True, # User must be logged in to send let these through
            'match': {
                'action': 'save',
                'collection': 'orders'
            }
        }
    ],

    # This defines which messages from the server we will let through to the client
    'outbound_permitted': [
        dict(address_re=r'hbbrowser\..+\..+'),
        dict(address_re=r'hbdriver\..+'),
    ]
}
