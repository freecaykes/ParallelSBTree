#!/usr/bin/env python

import os
import re
import sys
import math
import signal
import functools
import pickle
import copy
import time

from threading import Thread, Semaphore, Timer, Lock
from BTree import BTree
from Queue import Queue, Empty

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
        self.started = False
        self.threads = []
        self.leaves = []
        self.folded  = Queue(maxsize=1)
        self.final = Queue(maxsize=1)
        self.merge_lock = Lock()

    def foreach(self, function, root, merge_function=None, merge_args=None):
        if len(self.leaves) == 0:
            self.leaves = self._getleaves()

        p_leaves = self._group_siblings(self.leaves, [], merge_function, merge_args)

        self.started = True
        self._foreach_iter_(function, p_leaves,merge_function, merge_args)

        if merge_function:
            while self.final.empty():
                continue
            final_return = self.final.get()
            self.final.empty()
            return final_return
        else:
            return None

    def _group_siblings(self, leaves, values, merge_function, merge_args):
        if len(leaves) == 0:
            return []

        t_parents = {}
        t_values = {}
        for i, l in enumerate(leaves):
            # print count
            if t_parents.get(str(l.parent), -1) == -1:
                t_parents[str(l.parent)] = [l]
                if merge_function  and len(values) > 0:
                    t_values[str(l.parent)] = values[i]
            else:
                t_parents[str(l.parent)].append(l)
                if merge_function  and len(values) > 0:
                    t_values[str(l.parent)].append(values[i])

        p_leaves = []
        for key in t_parents.keys():
            _values = None
            if merge_function and len(values) > 0 : # attach the reduced sibling batch
                _values = self._merge_function_wrapper( merge_function, merge_args, values)
            p_leaves.append( (t_parents[key], _values) )

        return p_leaves

    def _foreach_iter_(self, function, p_leaves, merge_function, merge_args):
        _p_leaves = []
        _values = []
        num_leaves = len(p_leaves)
        end = False
        # print str( p_leaves )

        def _inner_thread_work(tree, s_batch, function, merge_function, merge_args, resource_queue):
            for leaf in s_batch:
                print "inner:", leaf
                worker_thread = Thread( target=tree._function_wrapper,
                    args=(function, merge_function, leaf, merge_args, resource_queue) )
                worker_thread.daemon = True
                worker_thread.start()
                worker_thread.join()
                tree.threads.append(worker_thread)

        for s_batch_tup in p_leaves:  # sibling batch
            s_batch = s_batch_tup[0]
            s_batch_value = s_batch_tup[1]

            s_parent = s_batch[0].parent
            print "TOP:", s_batch_tup, s_parent

            resource_queue = Queue() # unique to each s_batch
            # populate queue with node.contents
            worker_thread = Thread(target=_inner_thread_work,
                args=(self, s_batch, function, merge_function, merge_args, resource_queue))
            worker_thread.daemon = True
            worker_thread.start()
            worker_thread.join()

            if merge_function:# merge results with value from children
                self.folded.join() # block until _fold_results is done ie. self.folded is filled
                self._fold_results(resource_queue, num_leaves,merge_function, merge_args)

                folded = self.folded.get()
                self.folded.empty()
                s_b_fold_arr = [s_batch_value, folded]
                combined_value = self._merge_function_wrapper( merge_function, merge_args, list( filter( lambda t: t, s_b_fold_arr)) )

                _p_leaves.append( s_parent )
                _values.append( combined_value )
                if not s_parent:
                    print "DONE ROOT", str( combined_value )
                    end = True


        if end: # if root
            self.started = False
            temp = self.threads
            self.threads = []
            del temp
            if merge_function:
                self.final.put_nowait( _values[0] )
            return

        self.threads = list(filter(lambda t: t.is_alive(), self.threads)) # remove all dead threads
        ret_p_leaves = self._group_siblings( _p_leaves, _values, merge_function, merge_args )
        print "Nodes:", p_leaves, "Parents: ", ret_p_leaves
        self._foreach_iter_(function, ret_p_leaves, merge_function, merge_args) # recurse on parents

    def _function_wrapper(self, function, merge_function, root, merge_args, resource_queue):
        if merge_function: # if get fold
            _retvalues = list( map(lambda c: function(c), root.contents) )
            _fold_val = self._merge_function_wrapper(merge_function, merge_args, _retvalues)
            resource_queue.put(_fold_val, False)
            del _retvalues
            del _fold_val
        else:
            map(lambda c: function(c), root.contents)

    def _fold_results(self, resource_queue, num_leaves, merge_function, merge_args):
        if resource_queue.qsize() >= num_leaves:
            _mergevalues = list(resource_queue.queue)
            if len(_mergevalues) == 1:
                # end on root
                # print "DONE ROOT", str( _mergevalues[0] )
                self.folded.put_nowait( _mergevalues[0] )
            else:
                self.folded.put_nowait ( self._merge_function_wrapper(merge_function, merge_args, _mergevalues) )

            self.folded.task_done()
        else:  # runs on another thread
            # print "spawned"
            Timer(5e-5, self._fold_results, args=(resource_queue, num_leaves, merge_function, merge_args)).start()

    def _merge_function_wrapper(self, merge_function, merge_args, returnvalues):
        print "_merge_function_wrapper", str(returnvalues)
        if merge_function and len(returnvalues) > 0:
            try:
                if merge_args:
                    return reduce(lambda e1, e2: merge_function(e1, e2, merge_args), returnvalues)
                    print "HIT 1"
                else:
                    return reduce(lambda e1, e2: merge_function(e1, e2), returnvalues)
                    print "HIT 2"
            except TypeError, e:
                print "TypeError: ", str(e)
                return None


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

    def items(self):
        return list(self._psbt)

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
