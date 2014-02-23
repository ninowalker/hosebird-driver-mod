import vertx
from core.event_bus import EventBus
import uuid


logger = vertx.logger()


class CredentialManager(object):
    ADDRESS = 'hbdriver:credentials'

    def __init__(self, credentials, address=ADDRESS):
        self._pool = list(credentials)
        self._inuse = {}
        EventBus.register_handler(address, handler=self.handle)

    def handle(self, msg):
        cmd = msg.body['command']
        actor = msg.body['id']
        if cmd == 'acquire':
            try:
                creds = self._pool.pop(0)
                self._inuse[actor] = creds
                logger.info("%s has acquired credentials" % actor)
                msg.reply(dict(status=200, credentials=creds))
            except IndexError:
                msg.reply(dict(status=404, msg="No credentials available"))
        if cmd == 'release':
            try:
                creds = self._inuse.pop(actor)
                self._pool.append(creds)
                logger.info("%s has released credentials" % actor)
                msg.reply(dict(status=200))
            except KeyError:
                msg.reply(dict(status=404, msg="Unknown indentifier: %s" % actor))
        if cmd == 'add':
            self.add(msg.body['credentials'])
            
    def size(self):
        return len(self._pool) + len(self._inuse)
    
    def add(self, creds):
        if isinstance(creds, dict):
            creds = creds['consumerKey'], creds['consumerSecret'], creds['appToken'], creds['appSecret'] 
        self._pool.append(creds)
    
    def from_env(self, key):
        try:
            creds = vertx.env()[key]
            consumer_key, consumer_secret, app_token, app_secret = creds.split(" ")        
            self.add([consumer_key, consumer_secret, app_token, app_secret])
        except Exception, e:
            logger.error("Credentials could not be loaded from env var: %s: %s" % (key, e))
            raise