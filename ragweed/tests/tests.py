from cStringIO import StringIO
import ragweed.framework
import hashlib
import string
import random


from ragweed.framework import *

def rgwa():
    return ragweed.framework.ragweed_env.zone.rgw_rest_admin

def validate_obj_location(rbucket, obj):
    expected_head_pool = rbucket.get_data_pool()
    print 'expected head pool: ' + expected_head_pool

    obj_layout = rgwa().get_obj_layout(obj)

    print 'layout', obj_layout

    print 'head', obj_layout.head
    expected_tail_pool = rbucket.get_tail_pool(obj_layout)

    eq(obj_layout.head.pool, expected_head_pool)

    for o in obj_layout.data_location:
        print 'ofs=', o.ofs, 'loc', o.loc
        if o.ofs > 0 or o.loc.oid != obj_layout.head.oid:
            eq(o.loc.pool, expected_tail_pool)

def gen_rand_string(size, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

# stage:
# create objects in multiple sizes, with various names
# check:
# verify data correctness
# verify that objects were written to the expected data pool
class r_test_small_obj_data(RTest):
    def stage(self):
        self.r_obj_names = [ 'obj', '_', '__', '_ _' ]
        self.r_bucket_sizes = {}

        sizes = { 0, 512 * 1024, 1024 * 1024 }

        for size in sizes:
            rbucket = self.create_bucket()
            self.r_bucket_sizes[rbucket.name] = size
            data = '0' * size
            for n in self.r_obj_names:
                obj = Key(rbucket.bucket)
                obj.key = n;
                obj.set_contents_from_string(data)

    def check(self):
        print self.r_obj_names
        for rbucket in self.get_buckets():
            size = self.r_bucket_sizes[rbucket.name]
            data = '0' * int(size)

            for n in self.r_obj_names:
                obj = Key(rbucket.bucket)
                obj.key = n;
                obj_data = obj.get_contents_as_string()
                eq(data, obj_data)

                validate_obj_location(rbucket, obj)

# stage:
# init, upload, and complete a multipart object
# check:
# verify data correctness
# verify that object layout is correct
class r_test_multipart_simple(RTest):
    def stage(self):
        rb = self.create_bucket()
        self.r_obj = 'foo'

        num_parts = 3

        b = rb.bucket

        h = hashlib.md5()
        payload=gen_rand_string(5)*1024*1024
        mp = b.initiate_multipart_upload(self.r_obj)
        for i in range(0, num_parts):
            mp.upload_part_from_file(StringIO(payload), i+1)
            h.update(payload)

        last_payload='123'*1024*1024
        mp.upload_part_from_file(StringIO(last_payload), num_parts + 1)
        h.update(last_payload)

        mp.complete_upload()

        self.r_md5 = h.hexdigest()
        print 'written md5: ' + self.r_md5

    def check(self):
        for rb in self.get_buckets():
            break

        b = rb.bucket

        obj = b.get_key(self.r_obj)

        validate_obj_location(rb, obj)
        h = hashlib.md5()
        h.update(obj.get_contents_as_string())
        obj_md5 = h.hexdigest()
        print 'read md5: ' + obj_md5
        eq(obj_md5, self.r_md5)

