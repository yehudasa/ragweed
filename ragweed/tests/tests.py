import ragweed.framework

from ragweed.framework import *

def rgwa():
    return ragweed.framework.ragweed_env.rgw_rest_admin

class r_test_small_obj_data(RTest):
    def stage(self):
        self.r_obj_names = [ 'obj', '_', '__', '_ _' ]
        self.r_bucket_sizes = {}

        sizes = { 0, 512 * 1024, 1024 * 1024 }

        for size in sizes:
            bucket = self.create_bucket()
            ep = rgwa().get_bucket_instance_info(bucket.name)
            self.r_bucket_sizes[bucket.name] = size
            data = '0' * size
            for n in self.r_obj_names:
                obj = Key(bucket)
                obj.key = n;
                obj.set_contents_from_string(data)

                print rgwa().get_obj_layout(obj)

    def check(self):
        print self.r_obj_names
        for bucket in self.get_buckets():
            print bucket.name
            size = self.r_bucket_sizes[bucket.name]
            data = '0' * int(size)
            for n in self.r_obj_names:
                obj = Key(bucket)
                obj.key = n;
                obj_data = obj.get_contents_as_string()
                eq(data, obj_data)


