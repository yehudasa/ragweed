import sys
import os
import boto
import boto.s3.connection
import json
import inspect
import pickle
from boto.s3.key import Key
from nose.plugins.attrib import attr
from nose.tools import eq_ as eq

ragweed_env = None
suite = None

class RGWConnection:
    def __init__(self, access_key, secret_key, host):
        host, port = (host.rsplit(':', 1) + [None])[:2]
        if port:
            port = int(port)

        self.conn = boto.connect_s3(
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                host=host,
                port=port,
                is_secure=False,
                calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                )

    def create_bucket(self, name):
        return self.conn.create_bucket(name)

    def get_bucket(self, name):
        return self.conn.get_bucket(name)


class RSuite:
    def __init__(self, name, connection, suite_step):
        self.name = name
        self.conn = connection
        self.config_bucket = None
        self.rtests = []
        self.do_staging = False
        self.do_check = False
        for step in suite_step.split(','):
            if step == 'stage' or step == 'staging':
                self.do_staging = True
                self.config_bucket = self.create_bucket(self.get_bucket_name('conf'))
            if step == 'check' or step == 'test':
                self.do_check = True
                self.config_bucket = self.get_bucket(self.get_bucket_name('conf'))

    def get_bucket_name(self, suffix):
        return self.name + '-' + suffix

    def create_bucket(self, name):
        return self.conn.create_bucket(name)

    def get_bucket(self, name):
        return self.conn.get_bucket(name)

    def register_test(self, t):
        self.rtests.append(t)

    def write_test_data(self, test):
        k = Key(self.config_bucket)
        k.key = 'tests/' + test._name
        k.set_contents_from_string(test.to_json())

    def read_test_data(self, test):
        k = Key(self.config_bucket)
        k.key = 'tests/' + test._name
        s = k.get_contents_as_string()
        test.from_json(s)

    def is_staging(self):
        return self.do_staging

    def is_checking(self):
        return self.do_check


class RTestJSONSerialize(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (list, dict, str, unicode, int, float, bool, type(None))):
            return JSONEncoder.default(self, obj)
        return {'__pickle': pickle.dumps(obj)}

def rtest_decode_json(d):
    if '__pickle' in d:
        return pickle.loads(str(d['__pickle']))
    return d

class RTest:
    def __init__(self):
        self._name = self.__class__.__name__
        self.r_buckets = []

    def create_bucket(self):
        bid = len(self.r_buckets) + 1
        bucket = suite.create_bucket(suite.get_bucket_name(self._name + '-' + str(bid)))
        self.r_buckets.append(bucket.name)
        return bucket

    def get_buckets(self):
        for b in self.r_buckets:
            yield suite.get_bucket(b)

    def stage(self):
        pass

    def check(self):
        pass

    def to_json(self):
        attrs = {}
        for x in dir(self):
            if x.startswith('r_'):
                attrs[x] = getattr(self, x)
        return json.dumps(attrs, cls=RTestJSONSerialize)

    def from_json(self, s):
        j = json.loads(s, object_hook=rtest_decode_json)
        for e in j:
            setattr(self, e, j[e])

    def save(self):
        suite.write_test_data(self)

    def load(self):
        suite.read_test_data(self)

    def test(self):
        suite.register_test(self)
        if suite.is_staging():
            self.stage()
            self.save()

        if suite.is_checking():
            self.load()
            self.check()

class RagweedEnv:
    def __init__(self):
        access_key = os.environ['S3_ACCESS_KEY_ID']
        secret_key = os.environ['S3_SECRET_ACCESS_KEY']
        host = os.environ['S3_HOSTNAME']

        self.conn = RGWConnection(access_key, secret_key, host)
        self.suite = RSuite('ragweed', self.conn, os.environ['RAGWEED_RUN'])

        print self.suite



def setup_module():
    global ragweed_env
    global suite

    ragweed_env = RagweedEnv()
    suite = ragweed_env.suite
