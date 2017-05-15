import unittest
import random
import time

from ParallelSBTree  import ParallelSBTree

class test_simple(unittest.TestCase):

    def merge_function(self, head_value, add_sum):
        print "add_sum", add_sum

        if left_value:
            add_sum += left_value
        if right_value:
            add_sum += right_value


    def parallel_function(self, node):
        # simple square sum
        return node.value ** 2

    def test_pbst_filter(self):
        NUM_THREADS = 64
        to_sum = [int(1000*random.random()) for i in xrange(NUM_THREADS)]
        sum_dict = dict( (random.randint(100,999), rand_int) for rand_int in  to_sum)

        add_sum = 0
        num_trees = ParallelSBTree(sum_dict, shared=add_sum)
        print num_trees.psbt

        num_trees.foreach(self.parallel_function, num_trees.psbt._root, self.merge_function)

        # while num_trees.started:
        #     count += 1
        #     # print count
        #     continue
        print add_sum

        sum_native = sum(to_sum)
        self.assertEquals(add_sum, sum_native)

if __name__ == '__main__':
    unittest.main()
