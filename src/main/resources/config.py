from com.twitter.hbc import ClientBuilder
from com.twitter.hbc.core import Constants
from com.twitter.hbc.core.endpoint import StatusesFilterEndpoint, Location
from com.twitter.hbc.core.processor import StringDelimitedProcessor
from com.twitter.hbc.httpclient.auth import OAuth1
from java.util.concurrent import LinkedBlockingQueue
from juice import Juice
import vertx


logger = vertx.logger()


class ClientConfig(dict):
    def __init__(self, d=None, **kwargs):
        self.update(d or {})
        self.update(**kwargs)
        self['id'] = "%s-%s" % (self.get('name', 'unnamed'), id(self))
        self['driverAddress'] = "hbinstance.%s" % self['id']


class ClientFactory(object):
    def __init__(self, **options):
        self.options = options

    def __call__(self, client_cfg, pipeline, creds):
        endpoint = self._endpoint(client_cfg)
        #pipeline = Juice.pipeline_factory(client_cfg)
        processor = StringDelimitedProcessor(DelegateProxy(pipeline))
        hosts = self.options.get("streamHost", Constants.STREAM_HOST)

        auth = OAuth1(*creds)

        return ClientBuilder()\
          .hosts(hosts)\
          .endpoint(endpoint)\
          .authentication(auth)\
          .processor(processor)\
          .build()

    def _endpoint(self, cfg):
        endpoint = StatusesFilterEndpoint()
        endpoint.trackTerms(self._extract_terms(cfg))
        endpoint.followings(self._extract("id", cfg.get('follow', [])))
        locations = [Location(Location.Coordinate(w, s), Location.Coordinate(e, n))
                     for s, w, n, e in self._extract("bounds", cfg.get('location', []))]
        endpoint.locations(locations)
        return endpoint

    def _extract_terms(self, cfg):
        terms = [v.replace(" && ", " ") for v in self._extract("phrase", cfg.get('track', []))]
        for u in cfg.get('follow', []):
            # twitter does not deliver all mentions if simply following by id.
            if u.get('mentions'):
                terms.append("@%s" % u['screen_name'])
        return terms

    def _extract(self, key, list_):
        a = []
        for v in list_:
            if isinstance(v, dict):
                a.append(v[key])
            else:
                a.append(v)
        return a


class DelegateProxy(LinkedBlockingQueue):
    """Mocks the interface and instead calls func."""
    def __init__(self, func):
        self.func = func

    def offer(self, item, *_args):
        self.func(item)


#def defmain():
#    true = True
#    c = [{
#                      "track": [
#                          {"phrase": "san francisco", "tags": ["a", "b"]},
#                          {"phrase": "san francisco && giants", "tags": ["c"]},
#                          {"phrase": "san francisco && lolz", "tags": ["d"]}
#                      ],
#                      "follow": [
#                          {"id": 12345, "handle": "meow", "mentions": true, "tags": ["a"]}
#                      ],
#                      "location": [
#                          {"bounds": [0, 0, 1, 1], "rules": ["c"]}
#                      ],
#                      "name": "san-francisco",
#                      "id": "set at runtime",
#                      "type": "StatusesFilterEndpoint",
#                      "matching": true
#                  },
#                  {
#                      "track": [
#                          {"phrase": "lol"},
#                          {"phrase": "#lol && meow"},
#                          {"phrase": "meow"}
#                      ],
#                      "name": "test-2",
#                      "id": "set at runtime",
#                      "type": "StatusesFilterEndpoint"
#                  }
#    ]
#    return DriverConfiguration(c[0]), \
#        DriverConfiguration(c[1])
#
#cfgs = defmain()
#m = cfgs[0].matcher
#
#t = dict(user=dict(id=1234),
#         entities=dict(user_mentions=[{"id": 12345}]),
#         text="meow is for #lolz #lol catz cool fun word @andersoncooper Can we just get rid of Arizona? I don't want them making America look bad internationally anymore. Sochi treats gays better #sochi")
#
#print cfgs[1].matcher.search(t)

