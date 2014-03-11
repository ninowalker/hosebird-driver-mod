from com.googlecode.concurrenttrees.common import PrettyPrinter
from com.googlecode.concurrenttrees.radix.node.concrete import \
    DefaultCharArrayNodeFactory
from com.googlecode.concurrenttrees.radixinverted import \
    ConcurrentInvertedRadixTree
from java.lang import System
from collections import namedtuple
import re


MatchInfo = namedtuple("MatchInfo", "tags i n")


class CrudeFastTextMatcher(object):
    def __init__(self, pad=True):
        self.tree = ConcurrentInvertedRadixTree(DefaultCharArrayNodeFactory())
        self.pad = pad
        self.rss = re.compile(r'\s+')
        self.rb = re.compile(r'(\b)')
        
    def add(self, key, value):
        # value should be an iterable 
        self.tree.put(self.normalize_text(key), value)
        
    def search(self, tweet):
        string = self.normalize_text(tweet['text'])        
        return dict(((kv.key.strip(), kv.value) for kv in self.tree.getKeyValuePairsForKeysContainedIn(string)))
    
    def normalize_text(self, t):
        string = self.rss.sub(' ', self.rb.sub(r' \1 ', t.lower()))
        return string
    

class UserMatcher(object):
    def __init__(self):
        self.umap = {}
        
    def add(self, uid, value):
        # value should be an iterable
        self.umap[uid] = value
        
    def search(self, tweet):
        ids = set([tweet['user']['id']])
        ids.update((m['id'] for m in (tweet['entities'].get('user_mentions') or [])))
        if 'retweet_status' in tweet:
            ids.add(tweet['retweet_status']['user']['id'])
            ids.update((m['id'] for m in tweet['retweet_status']['entities'].get('user_mentions') or []))

        matched = {}
        for id in ids:
            try:
                matched[id] = self.umap[id]
            except KeyError:
                pass
        return matched


class PrettyGoodTweetMatcher(object):
    def __init__(self):
        self.um = UserMatcher()
        self.tm = CrudeFastTextMatcher()
        
    def add_phrase(self, p, values):
        self.tm.add(p, values)
        
    def add_user(self, uid, values):
        self.um.add(uid, values)
        
    def search(self, tweet):
        umap = self.um.search(tweet)
        tmap = self.tm.search(tweet)
        umap.update(tmap)
        return umap
    
    def normalize_text(self, t):
        return self.tm.normalize_text(t)
    
    def pretty_print(self):
        print "Phrase matcher:"
        PrettyPrinter.prettyPrint(self.tm.tree, System.out)