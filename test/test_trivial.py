import unittest
import random
import time

from parallelsbtree import ParallelSBTree

class test_simple(unittest.TestCase):

    def merge_function(self, e1, e2):
        return e1 + e2

    def parallel_function(self, element):
        # simple square sum
        # print "sqr func:", element
        return element ** 2 # if merge with

    def test_pbst_filter(self):
        NUM_THREADS = 64
        to_sum = [int(1000*random.random()) for i in xrange(NUM_THREADS)]
        num_trees = ParallelSBTree(to_sum, NUM_THREADS,None)

        add_sum = num_trees.foreach(self.parallel_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum(to_sum)
        self.assertEquals(add_sum, sum_native)

if __name__ == '__main__':
    unittest.main()
