# Example JavaScript integration test that deploys the module that this project builds.
# Quite often in integration tests you want to deploy the same module for all tests and you don't want tests
# to start before the module has been deployed.
# This test demonstrates how to do that.

# Import the VertxAssert class - this has the exact same API as JUnit
from org.vertx.testtools import VertxAssert
import java.lang.System
import vertx_tests
from core.event_bus import EventBus

import vertx

# The test methods must begin with "test"

from mod.matcher import PrettyGoodTweetMatcher, CrudeFastTextMatcher

def test_match():
    try:
from mod.matcher import PrettyGoodTweetMatcher, CrudeFastTextMatcher
import time, random
t = dict(user=dict(id=1234),
         entities=dict(user_mentions=[{"id": 12345}]),
         text="meow is for #lolz catz cool fun word @andersoncooper Can we just get rid of Arizona? I don't want them making America look bad internationally anymore. Sochi treats gays better #sochi")

word_file = "/usr/share/dict/words"
WORDS = open(word_file).read().splitlines()

m = CrudeFastTextMatcher(pad=True)
m.add("#lolz", 1)
m.add("#lol", 2)
m.add("lol", 3)
m.add("fun", 5)
m.add("@andersoncooper", 6)
m.add("@sochi", 6)
for w in random.sample(WORDS, 5000):
    m.add(w.lower(), w)

res = m.search(t)
s = time.time()
n = 100000
for i in xrange(n):
    res = m.search(t)

d = time.time() - s
print (d / n) * 10**6


m = PrettyGoodTweetMatcher()
for w in random.sample(WORDS, 5000):
    m.add_phrase(w.lower(), w)

m.add_phrase("catz cool", 1)
m.add_user(1234, 2)
for i in xrange(1000):
    m.add_user(i, i)

s = time.time()
n = 100000
for i in xrange(n):
    res = m.search(t)

d = time.time() - s
print (d / n) * 10**6

        print res
        VertxAssert.assertEquals(1, len(res))
        VertxAssert.assertEquals(1, res["#lolz"])
        VertxAssert.assertEquals(2, res["#lolz"])
    finally:
        VertxAssert.testComplete()
    
#    def handler(msg):
#        VertxAssert.assertEquals('pong!', msg.body)
#        VertxAssert.testComplete()
#    EventBus.send('ping-address', 'ping!', handler)

def test_something_else() :
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
#vertx.deploy_module(java.lang.System.getProperty("vertx.modulename"), None, 1, handler)

