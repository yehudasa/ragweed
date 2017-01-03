from ragweed.framework import *

class r_test_small_obj_data(RTest):
    def stage(self):
        self.r_obj_names = [ 'obj', '_', '__', '_ _' ]
        self.r_bucket_sizes = {}

        sizes = { 0, 512 * 1024, 1024 * 1024 }

        for size in sizes:
            bucket = self.create_bucket()
            self.r_bucket_sizes[bucket.name] = size
            data = '0' * size
            for n in self.r_obj_names:
                obj = Key(bucket)
                obj.key = n;
                obj.set_contents_from_string(data)

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


