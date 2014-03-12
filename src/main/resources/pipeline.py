import vertx
from collections import defaultdict
from core.event_bus import EventBus
from juice import Juice
from matcher import PrettyGoodTweetMatcher, MatchInfo
from org.apache.commons.collections.buffer import CircularFifoBuffer
from org.vertx.java.core.json import JsonObject
import random
import time


logger = vertx.logger()


class PipelineFactory(object):
    def __init__(self, pdesc):
        self.desc = pdesc

    def __call__(self, client_config):
        h = [HosebirdSpout(client_config)]
        for d in self.desc:
            # brittle for now: look at this module
            handler = globals()[d['name']]
            if not d.get('enabled', True):
                continue
            kwargs = d.get("options", {})
            h.append(handler(client_config, **kwargs))
        return Pipeline(h)


class Pipeline(object):
    def __init__(self, handlers):
        self.handlers = handlers
        self.pmap = {}
        last = None
        for p in handlers:
            p._pipeline = self
            p._last = last
            if last:
                last._next = p
            last = p
            self.pmap[p.__class__] = p

    def __call__(self, o):
        # kick off the first Handler
        try:
            self.handlers[0](o)
        except Exception, e:
            logger.error("Failure processing %s" % e)

    def start(self):
        [p.start() for p in self.handlers]

    def shutdown(self):
        [p.shutdown() for p in self.handlers]

    def stats(self):
        s = {}
        for p in self.handlers:
            s[p.__class__.__name__] = p.stats()
        return s

    def get(self, pclass):
        return self.pmap.get(pclass)


class PipelineHandler(object):
    _last = None
    _pipeline = None
    _next = None

    @property
    def last(self):
        return self._last

    @property
    def pipeline(self):
        return self._pipeline

    def start(self):
        pass

    def shutdown(self):
        pass

    def stats(self):
        return {"active": True}

    def reset(self):
        pass

    def next(self, o):
        if self._next:
            self._next(o)


class HosebirdSpout(PipelineHandler):
    shutdown_ms = 100

    def __init__(self, client_config, **options):
        self.client_config = client_config
        self.cycles = 0
        self.creds = None

    def stats(self):
        t = self.client.getStatsTracker()
        stats = {"cycles": self.cycles}
        for a, v in [('getNum200s', '200s'),
                  ('getNum400s', '400s'),
                  ('getNumDisconnects', 'disconnects'),
                  ('getNumConnects', 'connects'),
                  ('getNumConnectionFailures', 'conn_failures'),
                  ('getNumClientEventsDropped', 'events_dropped')]:
            stats[v] = getattr(t, a)()
        return stats

    def __call__(self, o):
        self.next(o)

    def start(self):
        self.creds = Juice.credential_manager.lease(self)
        self.client = Juice.client_factory(self.client_config, self.pipeline, self.creds)
        self.client.connect()

    def shutdown(self):
        Juice.credential_manager.release(self)
        self.client.stop(self.shutdown_ms)

    def reset(self):
        self.cycles += 1
        try:
            self.shutdown()
        except:
            pass
        self.start()


class JsonParser(PipelineHandler):
    def __init__(self, client_config, **options):
        pass

    def __call__(self, o):
        print type(o)
        self.next(JsonObject(o).toMap())


class Decrufter(PipelineHandler):
    # https://dev.twitter.com/docs/platform-objects/tweets
    _field_ditch_list = set(['possibly_sensitive_editable',
                         'source',
                         'favorite_count',
                         'withheld_copyright',
                         'favorited',
                         'retweeted',
                         'retweet_count',
                         'geo',
                         'truncated',
                         ])

    # https://dev.twitter.com/docs/platform-objects/users
    _user_field_ditch_list = set(['utc_offset',
                             'contributors_enabled',
                             'default_profile',
                             'follow_request_sent',
                             'following',
                             'geo_enabled',
                             'is_translator',
                             #'listed_count',
                             'notifications',
                             'profile_background_color',
                             'profile_background_image_url',
                             'profile_background_image_url_https',
                             'profile_background_tile',
                             'profile_image_url',
                             'profile_banner_url',
                             'profile_link_color',
                             'profile_sidebar_border_color',
                             'profile_sidebar_fill_color',
                             'profile_text_color',
                             'profile_use_background_image',
                             'show_all_inline_media',
                             'status',
                             'time_zone',
                             'utc_offset',
                             'withheld_in_countries',
                             'default_profile_image',
                             ])

    _if_falsy_delete = set(['in_reply_to_status_id',
                           'in_reply_to_status_id_str',
                           'in_reply_to_user_id',
                           'in_reply_to_user_id_str',
                           'in_reply_to_screen_name',
                           'place',
                           'contributors'
                           ])

    _user_if_falsy_delete = set(['location',
                                'url',
                                'description',
                                'is_translation_enabled',
                                'protected',
                                'verified',
                                ])

    def __init__(self, client_config, **options):
        pass

    def __call__(self, o):
        self.decruft(o)
        self.next(o)

    def decruft(self, o):
        if 'text' in o:
            for f in self._field_ditch_list:
                o.remove(f)
            for f in self._if_falsy_delete:
                if not o.get(f):
                    o.remove(f)
            if o.get('coordinates'):
                c = o['coordinates']['coordinates']
                if not c[0] and not c[1]:
                    o.remove('coordinates')
        if 'user' in o:
            u = o['user']
            for f in self._user_field_ditch_list:
                u.remove(f)
            for f in self._user_if_falsy_delete:
                if not u.get(f):
                    u.remove(f)

        if 'retweet_status' in o:
            self.decruft(o['retweet_status'])


class TagMatcher(PipelineHandler):
    def __init__(self, client_config, **_):
        self.matcher = self._matcher(client_config)

    def _matcher(self, cfg):
        m = PrettyGoodTweetMatcher()
        phrases = defaultdict(list)
        for kw in cfg.get('track', []):
            parts = kw['phrase'].split("&&")
            tags = kw.get('tags', kw['phrase'])
            for i, part in enumerate(parts):
                part = m.normalize_text(part)
                phrases[part.strip()].append(MatchInfo(tags, i + 1, len(parts)))

        users = defaultdict(list)
        for user in cfg.get('follow', []):
            tags = user['tags']
            users[user['id']].append(MatchInfo(tags, 1, 1))
            if user.get('mentions'):
                phrases["@%s" % user['screen_name']].append(MatchInfo(tags, 1, 1))
        for user, info in users.iteritems():
            m.add_user(user, info)
        for phrase, info in phrases.iteritems():
            m.add_phrase(phrase, info)
        return m

    def __call__(self, o):
        if 'text' in o:
            tags = self.matcher.search(o)
            o['_matches'] = tags
        self.next(o)

    def __str__(self):
        return str(self.matcher)


class RecentSampler(PipelineHandler):
    def __init__(self, client_config, **options):
        self.records = defaultdict(lambda: dict(count=0, recent=CircularFifoBuffer(options.get("bufferSize", 100)), last=None))
        self.sample_rate = float(options.get('tweetSampleRate', None))

    def __call__(self, map):
        stat = None

        if 'text' in map:
            if self.sample_rate and random.random() < self.sample_rate:
                stat = 'samples'
        elif 'limit' in map:
            stat = 'control.limit'
        elif 'warning' in map:
            stat = 'control.warning'
        elif 'disconnect' in map:
            stat = 'control.disconnect'

        if stat:
            self.records[stat]['last'] = time.time()
            self.records[stat]['count'] += 1
            self.records[stat]['recent'].add(map)
        self.next(map)

    def stats(self):
        s = dict(self.records)
        for k, v in s.iteritems():
            s[k] = dict(count=v['count'], last=v['last'])
        return s

    def recent(self, n):
        res = {}
        for k, v in self.records:
            res[k] = dict(v)
            res[k]['recent'] = list(res[k]['recent'])[0:n]
        return res


class StalledConnectionDetector(PipelineHandler):
    def __init__(self, client_config, **options):
        self.expected_rate = options.get("expectedEventsPerMinute", 1)
        self.last = time.time()

    def __call__(self, o):
        assert False


class Probe(PipelineHandler):
    TWEET = 1

    def __init__(self, client_config, **options):
        self.address = '%s.%s' % (client_config['instanceAddress'],
                                  options.get('name', 'trace'))

    def __call__(self, o):
        if 'text' in o:
            EventBus.publish(self.address, self.TWEET)
        elif 'limit' in o or 'warning' in o or 'disconnect' in o:
            EventBus.publish(self.address, o)
        self.next(o)


class Printer(PipelineHandler):
    def __call__(self, o):
        try:
            print "Received: ", type(o), o
        except:
            pass
        self.next(o)


class StreamGenerator(PipelineHandler):
    def __init__(self, client_config, **options):
        self.address = options.get('address') or '%s.%s' % (client_config['driverAddress'],
                                                            options.get('name', 'stream'))

    def __call__(self, o):
        #EventBus.publish(self.address, o)
        self.next(o)
