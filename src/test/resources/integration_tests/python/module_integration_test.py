# Example JavaScript integration test that deploys the module that this project builds.
# Quite often in integration tests you want to deploy the same module for all tests and you don't want tests
# to start before the module has been deployed.
# This test demonstrates how to do that.

# Import the VertxAssert class - this has the exact same API as JUnit
import vertx
from core.event_bus import EventBus
from org.vertx.testtools import VertxAssert
import java.lang.System
import random
import time
import vertx_tests


# The test methods must begin with "test"


def xxxxxtest_match():
#    from matcher import PrettyGoodTweetMatcher, CrudeFastTextMatcher

    try:
        pass
        t = dict(user=dict(id=1234),
                 entities=dict(user_mentions=[{"id": 12345}]),
                 text="meow is for #lolz catz cool fun word @andersoncooper Can we just get rid of Arizona? I don't want them making America look bad internationally anymore. Sochi treats gays better #sochi")

        word_file = "/usr/share/dict/words"
        WORDS = open(word_file).read().splitlines()

        m = CrudeFastTextMatcher(pad=True)
    finally:
        VertxAssert.testComplete()


def xxxxtest_something_else():
    VertxAssert.testComplete()

script = locals()


# The script is execute for each test, so this will deploy the module for each one
def handler(err, depID):
    # Deployment is asynchronous and this this handler will be called when it's complete (or failed)
    VertxAssert.assertNull(err)
    # If deployed correctly then start the tests!
    vertx_tests.start_tests(script)

# Deploy the module - the System property `vertx.modulename` will contain the name of the module so you
# don't have to hardecode it in your tests
vertx.deploy_module(java.lang.System.getProperty("vertx.modulename"), None, 1, handler)

