#!/usr/bin/env python

import os
import re
import sys
import math
import signal
import functools

import pickle
from threading import Thread, Semaphore, Timer
from BTree import BTree
from Queue import Queue

"""
    Parallel threading structure based off a Self-balancing Tree (B-Tree). For operations over
    the whole tree forming each parallel thread per node.
"""


class ParallelSBTree(object):

    def __init__(self, entries, order=100, shared=None):   # entries must be a  dicionary
        self.merge_lock = Semaphore(value=1)
        self.time_lock = Semaphore(value=1)
        self._psbt = BTree.bulkload(entries, order)
        self.shared = shared
        self.iterable = True
        self.threads = []
        self.started = False
        self.leaves = []

    def foreach(self, function, root, merge_function=None, merge_args=None):
        if len(self.leaves) == 0:
            self.leaves = self._getleaves()

        p_leaves = self._group_siblings(self.leaves, [], merge_function, merge_args)

        self.started = True
        folded = self._foreach_iter_(function, p_leaves,merge_function, merge_args)

        if merge_function:
            return folded
        else:
            return None


    def _group_siblings(self, leaves, values, merge_function, merge_args):
        t_parents = {}
        t_values = {}
        # print str(leaves), str(values)
        count = 0
        for l in leaves:
            # print count
            if t_parents.get(str(l.parent), -1) == -1:
                t_parents[str(l.parent)] = [l]
                if merge_function  and len(values) > 0:
                    t_values[str(l.parent)] = values[count]
            else:
                t_parents[str(l.parent)].append(l)
                if merge_function  and len(values) > 0:
                    t_values[str(l.parent)].append(values[count])
            count += 1

        p_leaves = []
        for key in t_parents.keys():
            _values = None
            if merge_function and len(values) > 0:
                _values = self._merge_function_wrapper( merge_function, merge_args, values)
            p_leaves.append( (t_parents[key], _values) )

        return p_leaves

    def _foreach_iter_(self, function, p_leaves, merge_function, merge_args):
        _p_leaves = []
        _values = []
        num_leaves = len(p_leaves)

        def _inner_thread_work(tree, s_batch, function, merge_function, merge_args, resource_queue):
            for leaf in s_batch:
                worker_thread = Thread( target=tree._function_wrapper,
                    args=(function, merge_function, leaf, merge_args, resource_queue) )
                worker_thread.start()
                tree.threads.append(worker_thread)
                # resource_queue.task_done()

        for s_batch_t in p_leaves:  # sibling batch
            s_batch = s_batch_t[0]
            s_batch_value = s_batch_t[1]

            s_parent = s_batch[0].parent
            resource_queue = Queue() # unique to each s_batch

            worker_thread = Thread(target=_inner_thread_work,
                args=(self, s_batch, function, merge_function, merge_args, resource_queue))
            worker_thread.start()
            resource_queue.join()  # resource_queue contains all to merge values per leaf in s_batch

            if merge_function:# merge results with parent
                self._fold_results(resource_queue, num_leaves,merge_function, merge_args)
                resource_queue.join()  # block until _fold_results is done
                combined_value = self._merge_function_wrapper( merge_function, merge_args, [s_batch_value, resource_queue.get(0)])
                _p_leaves.append(s_parent)
                _values.append( resource_queue.get(0) )

        if len(p_leaves) == 1 and p_leaves[0].parent == None:
            self.started = False
            temp = self.threads
            self.threads = []
            del temp
            if merge_function:
                return resource_queue.get(0)
            else:
                return

        ret_p_leaves = self._group_siblings( _p_leaves, _values, merge_function, merge_args )
        self.threads = list(filter(lambda t: t.is_alive(), self.threads))
        self._foreach_iter_(function, ret_p_leaves, merge_function, merge_args)

    def _function_wrapper(self, function, merge_function, root, merge_args, resource_queue):
        if merge_function: # if get fold
            _retvalues = list( map(lambda c: function(c), root.contents) )
            _fold_val = self._merge_function_wrapper(merge_function, merge_args, _retvalues)
            resource_queue.put(_fold_val)
            del _retvalues
            del _fold_val
        else:
            map(lambda c: function(c), root.contents)


    def _fold_results(self, resource_queue, num_leaves, merge_function, merge_args):
        if resource_queue.qsize() >= num_leaves:
            _mergevalues = list(resource_queue.queue)

            resource_queue.empty()
            resource_queue.put(self._merge_function_wrapper(merge_function, merge_args, _mergevalues))
            resource_queue.task_done()
        else:  # runs on another thread
            Timer(5e-7, self._fold_results, args=(resource_queue,num_leaves, merge_function, merge_args)).start()

    def _merge_function_wrapper(self, merge_function, merge_args, returnvalues):
        if merge_function and len(returnvalues) > 0:
            try:
                if merge_args:
                    return reduce(lambda e1, e2: merge_function(e1, e2, merge_args), returnvalues)
                else:
                    return reduce(lambda e1, e2: merge_function(e1, e2), returnvalues)
            except TypeError, e:
                print "TypeError: ", str(e)
                return None


    def items(self):
        return list(self._psbt)

    def _getleaves(self):
        leaves = []
        df_stack = [self._psbt._root]
        # print str(self._psbt)
        while len(df_stack) > 0:
            node = df_stack.pop()
            if len(node.children) == 0:
                leaves.append(node)
            else:
                map(lambda c: df_stack.append(c), node.children)

        return leaves

# ---------------- Node Operations ---------------- #

    def insert(self, item):
        self._psbt.insert(item)
        self.leaves=self._getleaves()

    def remove(self, key):
        self._psbt.remove(key)
        self.leaves=self._getleaves()

    def update_node(self, key):
        self.remove(key)
        self.insert(key, value)  # it has to be this way
        self.leaves=self._getleaves()
