#!/usr/bin/env python

#
#  get some freecaykes baby
#

import os
import re
import sys
import math
import signal

import pickle
# from bintrees import AVLTree
from threading import Thread, Semaphore, Timer
from BTree import BTree
from Queue import Queue

"""
    Parallel threading structure based off a Self-balancing Tree (B-Tree). For operations over
    the whole tree forming each parallel thread per node.
"""


class ParallelPerformanceTree(object):

    def __init__(self, entries, order=100, shared=None):   # entries must be a  dicionary
        self.merge_lock = Semaphore(value=1)
        self.time_lock = Semaphore(value=1)
        self._psbt = BTree(order)
        self.shared = shared
        self.iterable = True
        self.threads = []
        self.started = False
        self.queue = Queue()
        signal.signal(signal.SIGALRM, self.finish_merge)
        if entries:
            self.psbt.bulkload(entries)

    def foreach(self, function, root, merge_function=None, merge_args=None):
        if self.leaves:
            self.leaves = self._getleaves()

        t_parents = {}
        count = 0
        for l in leaves:
            if t_parents.get(str(l.parent), -1) == -1:
                t_parents[str(l.parent)] = [l]
            else:
                t_parents[str(l.parent)].append(l)

        p_leaves = []
        for key in t_parents.keys():
            p_leaves.append(t_parents[key])

        self.started = True
        self._foreach_iter_(self, function, p_leaves,
                            merge_function, merge_args)

    def _foreach_iter_(self, function, p_leaves, merge_function, merge_args):
        if len(p_leaves) == 1 and p_leaves[0].parent == None:
            self.started = False
            self.queue.empty()
            return

        # change value to args
        ret_p_leaves = []
        num_leaves = len(p_leaves)
        for s_batch in p_leaves:  # sibling batch
            s_parent = s_batch[0].parent
            resource_queue = Queue()

            def _inner_thread_work(tree, s_batch, function, p_leaves, merge_function, merge_args, resource_queue):
                for leaf in s_batch:
                    worker_thread = Thread(target=tree._function_wrapper, args=(
                        function, merge_function, leaf, merge_args, resource_queue))
                    worker_thread.start()
                    tree.threads.append(worker_thread)
                    resource_queue.task_done()

            worker_thread = Thread(target=_inner_thread_work, args=(s_batch, function, merge_function, leaf, merge_args, resource_queue))
            worker_thread.start()
            resource_queue.join()  # resource_queue contains all to merge values per leaf in s_batch

            if merge_function:# merge results with parent
                self._fold_results(resource_queue, num_leaves,merge_function, merge_args, s_parent)
                resource_queue.join()  # block until _fold_results is done

            ret_p_leaves.append(s_parent)

        self.threads = list(filter(lambda t: t.is_alive(), self.threads))
        self._foreach_iter_(function, ret_p_leaves, merge_function, merge_args)

    def _function_wrapper(self, function, merge_function, root, merge_args, resource_queue):
        _retvalues = list(map(lambda c: function(c), root.contents))
        if merge_function:
            _fold_val = self._merge_function_wrapper(merge_function, merge_args, _retvalues)
            resource_queue.put(_fold_val)

    def _fold_results(self, resource_queue, num_leaves, merge_function, merge_args, s_parent):
        if resource_queue.qsize() >= num_leaves:
            _mergevalues = iter(resource_queue.get, None)
            _mergevalues.extend(s_parent.contents)

            resource_queue.empty()
            resource_queue.put(self._merge_function_wrapper(
                merge_function, merge_args, _mergevalues))
            resource_queue.task_done()
        else:  # runs on another thread
            Timer(5e-7, self._join_results, args=(resource_queue,num_leaves, merge_function, merge_args, s_parent)).start()

    def _merge_function_wrapper(self, merge_function, merge_args, returnvalues):
        if merge_function:
            try:
                if merge_args:
                    return reduce(lambda e1, e2: merge_function(e1, e2, merge_args), retvalues)
                else:
                    return reduce(lambda e1, e2: merge_function(e1, e2), retvalues)
            except TypeError, e:
                print "TypeError: ", str(e)
                return None


    def items(self):
        return list(self.psbt)

    def _getleaves(self):
        leaves = []
        dfs_stack = [self.psbt._root]
        n = len(df_stack)
        while n > 0:
            node = df_stack[n - 1]
            df_stack = df_stack[0: n - 2]  # pop
            if len(node.children) == 0:
                leaves.append(node)
            else:
                map(lambda c: df_stack.append(c), node.children)

        return leaves

# ---------------- Node Operations ---------------- #

    def insert(self, item):
        self.psbt.insert(item)
        self.leaves=self._getleaves()

    def remove(self, key):
        self.psbt.remove(key)
        self.leaves=self._getleaves()

    def update_node(self, key):
        self.remove(key)
        self.insert(key, value)  # it has to be this way
        self.leaves=self._getleaves()
