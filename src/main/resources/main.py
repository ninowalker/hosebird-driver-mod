import vertx # this must be at the top of the file.
from config import ClientFactory, ClientConfig
from core.event_bus import EventBus
from driver import WebController, CredentialManager, Secretary
from juice import Juice
from pipeline import PipelineFactory


logger = vertx.logger()


def init(cfg):
    if cfg.get('debug') == True:
        EventBus.publish('hbdriver.multicast', dict(command="shutdown"))

    if cfg.get('webserver') is not None:
        WebController(cfg.get('webserver'))

    Juice.credential_manager = CredentialManager.init(cfg)
    Juice.pipeline_factory = PipelineFactory(cfg['pipeline'])
    Juice.client_factory = ClientFactory(**cfg.get('clients', {}))
    Juice.secretary = Secretary()

    Juice.client_factory(ClientConfig(track=[dict(phrase="meow")]),
                         lambda _: 1, ["a", "b", "c", "d"])

    logger.info("Loaded HBDriver module")

    for i, c in enumerate(cfg.get('autostart', [])):
        if not c.get('enabled', True):
            logger.info("Skipping disabled config #%d" % i)
            continue
        logger.info("Spawning client for config %s" % (c.get('name', i)))
        EventBus.send('hbdriver.new', dict(command="new", config=c),
                      lambda msg: logger.info("Started hosebird for config #%d: %s" % (i, msg.body)))


if __name__ == '__main__':
    init(vertx.config())
