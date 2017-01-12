from cStringIO import StringIO
import ragweed.framework
import binascii
import string
import random


from ragweed.framework import *

def rgwa():
    return ragweed.framework.ragweed_env.zone.rgw_rest_admin

def get_pool_ioctx(pool_name):
    return ragweed.framework.ragweed_env.rados.open_ioctx(pool_name)

def validate_obj_location(rbucket, obj):
    expected_head_pool = rbucket.get_data_pool()
    head_pool_ioctx = get_pool_ioctx(expected_head_pool)
    print 'expected head pool: ' + expected_head_pool

    obj_layout = rgwa().get_obj_layout(obj)

    print 'layout', obj_layout

    print 'head', obj_layout.head
    expected_tail_pool = rbucket.get_tail_pool(obj_layout)
    tail_pool_ioctx = get_pool_ioctx(expected_tail_pool)

    eq(obj_layout.head.pool, expected_head_pool)

    # check rados object for head exists
    head_pool_ioctx.set_locator_key(obj_layout.head.loc)
    (size, mtime) = head_pool_ioctx.stat(obj_layout.head.oid)
    print 'head size:', size, 'mtime:', mtime

    # check tail
    for o in obj_layout.data_location:
        print 'o=', o
        print 'ofs=', o.ofs, 'loc', o.loc
        if o.ofs > 0 or o.loc.oid != obj_layout.head.oid:
            eq(o.loc.pool, expected_tail_pool)

        # validate rados object exists
        tail_pool_ioctx.set_locator_key(o.loc.loc)
        (size, mtime) = tail_pool_ioctx.stat(o.loc.oid)

        eq(size, o.loc_ofs + o.loc_size)


def validate_obj(rbucket, obj_name, expected_crc):
    b = rbucket.bucket

    obj = b.get_key(obj_name)

    validate_obj_location(rbucket, obj)
    crc = binascii.crc32(obj.get_contents_as_string())
    obj_crc = '{:#010x}'.format(crc)
    print 'read crc: ' + obj_crc
    eq(obj_crc, expected_crc)


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

class MultipartUploaderState:
    def __init__(self, mu):
        self.upload_id = mu.mp.id
        self.crc = mu.crc


class MultipartUploader:
    def __init__(self, rbucket, obj_name, size, part_size, state=None):
        self.rbucket = rbucket
        self.obj_name = obj_name
        self.size = size
        self.part_size = part_size
        self.crc = 0

        if state is not None:
            self.crc = state.crc

            for upload in rbucket.bucket.list_multipart_uploads():
                if upload.key_name == self.obj_name and upload.id == state.upload_id:
                    self.mp = upload

    def prepare(self):
        self.mp = self.rbucket.bucket.initiate_multipart_upload(self.obj_name)
        self.crc = 0

    def upload(self):
        num_parts = self.size / self.part_size

        payload=gen_rand_string(self.part_size / (1024 * 1024))*1024*1024

        for i in xrange(0, num_parts):
            self.mp.upload_part_from_file(StringIO(payload), i + 1)
            self.crc = binascii.crc32(payload, self.crc)


        last_part_size = self.size % self.part_size

        if last_part_size > 0:
            last_payload='1'*last_part_size

            self.mp.upload_part_from_file(StringIO(last_payload), num_parts + 1)
            self.crc = binascii.crc32(last_payload, self.crc)

    def complete(self):
        self.mp.complete_upload()

    def hexdigest(self):
        return '{:#010x}'.format(self.crc)

    def get_state(self):
        return MultipartUploaderState(self)



# stage:
# init, upload, and complete a multipart object
# check:
# verify data correctness
# verify that object layout is correct
class r_test_multipart_simple(RTest):
    def stage(self):
        rb = self.create_bucket()
        self.r_obj = 'foo'

        uploader = MultipartUploader(rb, self.r_obj, 18 * 1024 * 1024, 5 * 1024 * 1024)

        uploader.prepare()
        uploader.upload()
        uploader.complete()

        self.r_crc = uploader.hexdigest()
        print 'written crc: ' + self.r_crc

    def check(self):
        for rb in self.get_buckets():
            break

        validate_obj(rb, self.r_obj, self.r_crc)


# stage:
# init, upload multipart object
# check:
# complete multipart
# verify data correctness
# verify that object layout is correct
class r_test_multipart_defer_complete(RTest):
    def init(self):
        self.obj_size = 18 * 1024 * 1024
        self.part_size = 5 * 1024 * 1024

    def stage(self):
        rb = self.create_bucket()
        self.r_obj = 'foo'

        uploader = MultipartUploader(rb, self.r_obj, self.obj_size, self.part_size)

        uploader.prepare()
        uploader.upload()

        self.r_upload_state = uploader.get_state()


    def check(self):
        for rb in self.get_buckets():
            break

        uploader = MultipartUploader(rb, self.r_obj, self.obj_size, self.part_size,
                                     state=self.r_upload_state)

        uploader.complete()
        crc = uploader.hexdigest()
        print 'written crc: ' + crc

        validate_obj(rb, self.r_obj, crc)

