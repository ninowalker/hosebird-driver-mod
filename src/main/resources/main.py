#@PydevCodeAnalysisIgnore
import vertx # this must be at the top of the file.

from config import ClientFactory, ClientConfig
from core.event_bus import EventBus
from driver import Director, WebController, CredentialManager
from pipeline import PipelineFactory
from juice import Juice


logger = vertx.logger()


def init(cfg):
    if cfg.get('debug') == True:
        EventBus.send('hbdriver.multicast', 
                      dict(command="shutdown"))

    if cfg.get('webserver') is not None:
        WebController(cfg.get('webserver'))
        
    Juice.credential_manager = CredentialManager.init(cfg)
    Juice.pipeline_factory = PipelineFactory(cfg['pipeline'])
    Juice.client_factory = ClientFactory(**cfg.get('clients', {}))
    Juice.director = Director()
    
    Juice.client_factory(ClientConfig(track=[dict(phrase="meow")]),
                         None,
                         ["a", "b", "c", "d"])
    
    logger.info("Loaded HBDriver module")
    
    for i, c in enumerate(cfg.get('autostart', [])):
        if not c.get('enabled', True):
            logger.info("Skipping disabled config #%d" % i)
            continue
        EventBus.send('hbdriver', dict(command="new",
                                       config=c), lambda msg: logger.info("Started hosebird for config #%d: %s" % (i, msg.body)))


if __name__ == '__main__':
    init(vertx.config())
