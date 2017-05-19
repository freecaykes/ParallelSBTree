import unittest
import random
import time

from parallelsbtree import ParallelSBTree

class test_simple(unittest.TestCase):

    def merge_function(self, e1, e2):
        # print "merged: ", str( e1 + e2 )
        return e1 + e2

    def parallel_function(self, element):
        # simple square sum
        # print "sqr func:", element
        return element ** 2 # if merge with

    def none_function(self, element):
        # simple square sum
        return element

    def test_pbst_large_order(self):
        NUM_THREADS = 64
        to_sum = [int(1000*random.random()) for i in xrange(NUM_THREADS)]
        num_trees = ParallelSBTree(to_sum, 10, None)

        # blocking until done
        add_sum = num_trees.foreach(self.parallel_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum( (list ( map (lambda x: x**2, to_sum))))
        # print sum_native
        self.assertEqual(add_sum, sum_native)

    def test_pbst_small_order(self):
        NUM_THREADS = 10
        to_sum = [int(1000*random.random()) for i in xrange(NUM_THREADS)]
        num_trees = ParallelSBTree(to_sum, 3, None)
        print to_sum
        print str(num_trees._psbt)
        # blocking until done
        add_sum = num_trees.foreach(self.none_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum( to_sum )
        # print sum_native
        self.assertEqual(add_sum, sum_native)

if __name__ == '__main__':
    unittest.main()
