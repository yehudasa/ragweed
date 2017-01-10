import ragweed.framework

from ragweed.framework import *

def rgwa():
    return ragweed.framework.ragweed_env.zone.rgw_rest_admin

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

            # bucket_placement_pool = 
            for n in self.r_obj_names:
                obj = Key(rbucket.bucket)
                obj.key = n;
                obj_data = obj.get_contents_as_string()
                eq(data, obj_data)

                obj_layout = rgwa().get_obj_layout(obj)

                print 'bucket_info', rbucket.bucket_info

                print 'head', obj_layout.head
                for o in obj_layout.data_location:
                    print 'ofs=', o.ofs, 'loc', o.loc
                print 'data_location', obj_layout.data_location

