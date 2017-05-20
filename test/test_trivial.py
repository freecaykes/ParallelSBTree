import unittest
import random
import time

from parallelsbtree import ParallelSBTree

class test_simple(unittest.TestCase):

    NUM_THREADS = 100000

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

    def test_pbst_small_set(self):
        small_num_T = 10
        to_sum = [int(1000*random.random()) for _ in xrange(small_num_T)]
        num_trees = ParallelSBTree(to_sum, 3, None)

        # blocking until done
        add_sum = num_trees.foreach(self.parallel_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum( (list ( map (lambda x: x**2, to_sum))))
        # print sum_native
        self.assertEqual(add_sum, sum_native)

    def test_pbst_1_leaf(self):
        to_sum = [int(1000*random.random()) for _ in xrange(self.NUM_THREADS)]
        num_trees = ParallelSBTree(to_sum, 1000, None)

        # blocking until done
        add_sum = num_trees.foreach(self.parallel_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum( (list ( map (lambda x: x**2, to_sum))))
        # print sum_native
        self.assertEqual(add_sum, sum_native)

    def test_pbst_large_order(self):
        to_sum = [int(1000*random.random()) for _ in xrange(self.NUM_THREADS)]
        num_trees = ParallelSBTree(to_sum, 10, None)

        # blocking until done
        add_sum = num_trees.foreach(self.parallel_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum( (list ( map (lambda x: x**2, to_sum))))
        # print sum_native
        self.assertEqual(add_sum, sum_native)

    def test_pbst_small_order(self):
        to_sum = [int(1000*random.random()) for _ in xrange(self.NUM_THREADS)]
        num_trees = ParallelSBTree(to_sum, 3, None)

        # blocking until done
        add_sum = num_trees.foreach(self.none_function, num_trees._psbt._root, self.merge_function)

        sum_native = sum( to_sum )
        # print sum_native
        self.assertEqual(add_sum, sum_native)

if __name__ == '__main__':
    unittest.main()
