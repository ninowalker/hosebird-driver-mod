import vertx # this must be at the top of the file.
from hbdriver import init_controller, init_autostart, init_credmgr, init_test_setup

logger = vertx.logger()
config = vertx.config()

init_credmgr(config)
init_controller(config)
init_test_setup(config)
init_autostart(config)
logger.info("Loaded HBDriver module")