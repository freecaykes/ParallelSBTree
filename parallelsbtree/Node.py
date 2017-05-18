#!/usr/bin/env python

import bisect
import itertools
import operator
import math


class _Node(object):
    """Internal object, represents a tree node."""
    __slots__ = ["tree", "contents", "children", "parent"]

    def __init__(self, tree, contents=[], children=[], parent=None):
        self.tree = tree
        self.contents = contents
        self.children = children
        self.parent = parent
        if self.children:
            # print "children", len(self.children), "contents", len(self.contents), "child", str(self.children), "contents", str(self.contents)
            assert len(self.contents) + 1 == len(self.children), "one more child than data item required"

    def __repr__(self):
        name = getattr(self, "children", 0) and "Branch" or "Leaf"
        return "<%s %s>" % (name, ", ".join(map(str, self.contents)))

    def insert(self, index, item, ancestors):
        # print "======================================="
        # print str(self.tree), "\n", str(self.children)
        # print "======================================="

        self.contents.insert(index, item)
        if len(self.contents) > self.tree.order:
            self._redistribute(ancestors)

    def remove(self, index, ancestors):
        minimum = self.tree.order // 2

        if self.children:
            # try promoting from the right subtree first,
            # but only if it won't have to resize
            additional_ancestors = [(self, index + 1)]
            descendent = self.children[index + 1]
            while descendent.children:
                additional_ancestors.append((descendent, 0))
                descendent = descendent.children[0]
            if len(descendent.contents) > minimum:
                ancestors.extend(additional_ancestors)
                self.contents[index] = descendent.contents[0]
                descendent.remove(0, ancestors)
                return
        # fall back to the left child
        additional_ancestors = [(self, index)]
        descendent = self.children[index]
        while descendent.children:
            additional_ancestors.append(
                (descendent, len(descendent.children) - 1))
            descendent = descendent.children[-1]
            ancestors.extend(additional_ancestors)
            self.contents[index] = descendent.contents[-1]
            descendent.remove(len(descendent.children) - 1, ancestors)
        else:
            self.contents.pop(index)
            if len(self.contents) < minimum and ancestors:
                self.grow(ancestors)


    def _redistribute(self, ancestors):
        parent = None

        if ancestors:
            parent, parent_index = ancestors.pop()
            # try to lend to the left neighboring sibling
            if parent_index:
                left_sib = parent.children[parent_index - 1]
                if len(left_sib.contents) < self.tree.order:
                    self._distribute(parent, parent_index,left_sib, parent_index - 1)
                    return

            # try the right neighbor
            if parent_index + 1 < len(parent.children):
                right_sib = parent.children[parent_index + 1]
                if len(right_sib.contents) < self.tree.order:
                    self._distribute(parent, parent_index, right_sib, parent_index + 1)
                    return

        center = len(self.contents) // 2
        sibling, push_up = self._split()

        if not parent:
            # print "GOTHERE"
            parent, parent_index = self.tree.BRANCH(self.tree, contents=[], children=[self]), 0
            self.tree._root = parent

        # pass the median up to the parent
        parent.contents.insert(parent_index, push_up)
        parent.children.insert(parent_index + 1, sibling)

        # update parent after _split
        sibling.parent = parent

        if len(parent.contents) > parent.tree.order:
            parent.shrink(ancestors)

    def grow(self, ancestors):
        parent, parent_index = ancestors.pop()

        minimum = self.tree.order // 2
        left_sib = right_sib = None

        # try to borrow from the right sibling
        if parent_index + 1 < len(parent.children):
            right_sib = parent.children[parent_index + 1]
            if len(right_sib.contents) > minimum:
                right_sib._distribute(parent, parent_index + 1, self, parent_index)
                return

        # try to borrow from the left sibling
        if parent_index:
            left_sib = parent.children[parent_index - 1]
            if len(left_sib.contents) > minimum:
                left_sib._distribute(parent, parent_index - 1, self, parent_index)
                return

        # consolidate with a sibling - try left first
        if left_sib:
            left_sib.contents.append(parent.contents[parent_index - 1])
            left_sib.contents.extend(self.contents)
            if self.children:
                left_sib.children.extend(self.children)
            parent.contents.pop(parent_index - 1)
            parent.children.pop(parent_index)
        else:
            self.contents.append(parent.contents[parent_index])
            self.contents.extend(right_sib.contents)
            if self.children:
                self.children.extend(right_sib.children)
            parent.contents.pop(parent_index)
            parent.children.pop(parent_index + 1)

        if len(parent.contents) < minimum:
            if ancestors:
                # parent is not the root
                parent.grow(ancestors)
            elif not parent.contents:
                # parent is root, and its now empty
                self.tree._root = left_sib or self
                self.tree._root.parent = None

    #
    # moves item from parent_index to dest_index
    #
    def _distribute(self, parent, parent_index, dest, dest_index):
        if parent:
            self.parent = parent
        if parent_index > dest_index:  # left tree
            dest.contents[dest_index] = parent.contents[dest_index]
            parent.contents[dest_index] = self.contents.pop(0)
            if self.children:
                dest.children.append(self.children.pop(0))
        else:  # right tree
            dest.contents[0] = parent.contents[parent_index]
            parent.contents[parent_index] = self.contents.pop()
            if self.children:
                dest.children.insert(0, self.children.pop())


    def _split(self):
        center = len(self.contents) // 2
        median = self.contents[center]
        sibling = type(self)(
            self.tree,
            self.contents[center + 1:],
            self.children[center + 1:])
        self.contents = self.contents[:center]
        self.children = self.children[:center + 1]
        return sibling, median
