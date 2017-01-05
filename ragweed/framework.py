import sys
import os
import boto
import boto.s3.connection
import json
import inspect
import pickle
import bunch
import yaml
import ConfigParser
from boto.s3.key import Key
from nose.plugins.attrib import attr
from nose.tools import eq_ as eq

from .reqs import _make_admin_request

ragweed_env = None
suite = None

class RGWConnection:
    def __init__(self, access_key, secret_key, host, port, is_secure):
        self.host = host
        self.port = port
        self.is_secure = is_secure
        self.conn = boto.connect_s3(
                aws_access_key_id = access_key,
                aws_secret_access_key = secret_key,
                host=host,
                port=port,
                is_secure=is_secure,
                calling_format = boto.s3.connection.OrdinaryCallingFormat(),
                )

    def create_bucket(self, name):
        return self.conn.create_bucket(name)

    def get_bucket(self, name, validate=True):
        return self.conn.get_bucket(name, validate=validate)


class RGWRESTAdmin:
    def __init__(self, connection):
        self.conn = connection

    def get_resource(self, path, params):
        r = _make_admin_request(self.conn, "GET", path, params)
        if r.status != 200:
            raise boto.exception.S3ResponseError(r.status, r.reason)
        return bunch.bunchify(json.loads(r.read()))


    def read_meta_key(self, key):
        return self.get_resource('/admin/metadata', {'key': key})

    def get_bucket_entrypoint(self, bucket_name):
        return self.read_meta_key('bucket:' + bucket_name)

    def get_bucket_instance_info(self, bucket_name, bucket_id = None):
        if not bucket_id:
            ep = self.get_bucket_entrypoint(bucket_name)
            print ep
            bucket_id = ep.data.bucket.bucket_id
        return self.read_meta_key('bucket.instance:' + bucket_name + ":" + bucket_id)

    def get_zone_params(self):
        return self.get_resource('/admin/config', {'type': 'zone'})


class RSuite:
    def __init__(self, name, conn, suite_step):
        self.name = name
        self.conn = conn
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
        return self.conn.regular.create_bucket(name)

    def get_bucket(self, name):
        return self.conn.regular.get_bucket(name)

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

def read_config(fp):
    config = bunch.Bunch()
    g = yaml.safe_load_all(fp)
    for new in g:
        print bunch.bunchify(new)
        config.update(bunch.bunchify(new))
    return config

str_config_opts = [
                'user_id',
                'access_key',
                'secret_key',
                'host',
                ]

int_config_opts = [
                'port',
                ]

bool_config_opts = [
                'is_secure',
                ]

def dict_find(d, k):
    if d.has_key(k):
        return d[k]
    return None

class RagweedEnv:
    def __init__(self):
        self.config = bunch.Bunch()

        cfg = ConfigParser.RawConfigParser()
        try:
            path = os.environ['RAGWEED_CONF']
        except KeyError:
            raise RuntimeError(
                'To run tests, point environment '
                + 'variable RAGWEED_CONF to a config file.',
                )
        with file(path) as f:
            cfg.readfp(f)

        for section in cfg.sections():
            try:
                (section_type, name) = section.split(None, 1)
                if not self.config.has_key(section_type):
                    self.config[section_type] = bunch.Bunch()
                self.config[section_type][name] = bunch.Bunch()
                cur = self.config[section_type]
            except ValueError:
                section_type = ''
                name = section
                self.config[name] = bunch.Bunch()
                cur = self.config

            cur[name] = bunch.Bunch()

            for var in str_config_opts:
                try:
                    cur[name][var] = cfg.get(section, var)
                except ConfigParser.NoOptionError:
                    pass

            for var in int_config_opts:
                try:
                    cur[name][var] = cfg.getint(section, var)
                except ConfigParser.NoOptionError:
                    pass

            for var in bool_config_opts:
                try:
                    cur[name][var] = cfg.getboolean(section, var)
                except ConfigParser.NoOptionError:
                    pass

        print json.dumps(self.config)

        rgw_conf = self.config.rgw

        self.conn = bunch.Bunch()

        for (k, u) in self.config.user.iteritems():
            self.conn[k] = RGWConnection(u.access_key, u.secret_key, rgw_conf.host, dict_find(rgw_conf, 'port'), dict_find(rgw_conf, 'is_secure'))

        self.suite = RSuite('ragweed', self.conn, os.environ['RAGWEED_RUN'])
        self.rgw_rest_admin = RGWRESTAdmin(self.conn.system)

        self.zone_params = self.rgw_rest_admin.get_zone_params()

        print 'zone_params:', self.zone_params




def setup_module():
    global ragweed_env
    global suite

    ragweed_env = RagweedEnv()
    suite = ragweed_env.suite
