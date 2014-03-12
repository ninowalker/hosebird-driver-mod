from matcher import PrettyGoodTweetMatcher, CrudeFastTextMatcher
from org.vertx.testtools import VertxAssert
import random
import time
import vertx_tests


def test_match():
    try:
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
        n = 10000
        for i in xrange(n):
            res = m.search(t)

        d = time.time() - s
        print "Processing rate per item: ", (d / n) * 10 ** 6, "micros"

        m = PrettyGoodTweetMatcher()
        for w in random.sample(WORDS, 5000):
            m.add_phrase(w.lower(), w)

        m.add_phrase("catz cool", 1)
        m.add_user(1234, 2)
        m.add_phrase("#lolz", 3)
        for i in xrange(1000):
            m.add_user(i, i)

        s = time.time()
        n = 10000
        for i in xrange(n):
            res = m.search(t)

        d = time.time() - s
        print "Processing rate per item: ", (d / n) * 10 ** 6, "micros"

        print m.normalize_text(t['text'])
        lolz_norm = m.normalize_text('#lolz')
        print res
        VertxAssert.assertEquals(3, len(res))
        VertxAssert.assertEquals(3, res[m.normalize_text(u'#lolz')])
        VertxAssert.assertEquals(1, res[m.normalize_text(u'cool catz')])

    finally:
        VertxAssert.testComplete()

vertx_tests.start_tests(locals())