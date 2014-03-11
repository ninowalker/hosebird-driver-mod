import vertx # this must be at the top of the file.
from credmgr import CredentialManager
from controller import start_server


logger = vertx.logger()


def init_controller(config):
        start_server(config['webserver'])



        

def init_autostart(config):


def init_test_setup(config):
    if not config.get('testmode', False):
        return 

    def register(addr):
        def _wrapper(func):
            EventBus.register_handler(addr, handler=func)
            return func
        return _wrapper

    @register("test.events")
    def event_handler(msg):
        print "event >>>", msg.body

    @register("test.tweets")
    def tweet_handler(msg):
        print ">>>", msg.body.get('id')

    @functools.partial(vertx.set_periodic, 10000)
    def status(tid):
        EventBus.publish(HosebirdDriver.MULTICAST, dict(command="status",
                                                  replyTo="test.status"))
        
    def subscribe(tid):
        EventBus.publish(HosebirdDriver.MULTICAST, dict(command="subscribe",
                                                          type="stream", 
                                                          replyTo="test.tweets"))
        EventBus.publish(HosebirdDriver.MULTICAST, dict(command="subscribe",
                                                          type="events", 
                                                          replyTo="test.events"))

    # give us 
    #vertx.set_timer(2000, subscribe)

    @register('test.status')
    def print_status(msg):
        a = msg.body['address']
        print a, "status >>>", msg.body

    @register('test.shutdown')
    def on_shutdown(msg):
        print "Shutdown >>>", msg.body
        vertx.exit()
        
    # simple shutdown hook
    exit_after = config.get('testmode').get('exitAfter', 10)
    if exit_after:
        vertx.set_timer(exit_after * 1000, 
                        lambda x: EventBus.publish(HosebirdDriver.MULTICAST, 
                                                   dict(command="shutdown",
                                                        replyTo="test.shutdown")))



init_credmgr(config)
init_controller(config)
init_test_setup(config)
init_autostart(config)
logger.info("Loaded HBDriver module")