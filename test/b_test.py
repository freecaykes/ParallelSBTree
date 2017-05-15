import random
import unittest

from BTree import BTree

class BTreeTests(unittest.TestCase):

    # def test_additions(self):
    #     bt = BTree(20)
    #     l = range(40)
    #     for i, item in enumerate(l):
    #         bt.insert(item)
    #         # print "children", len(bt.children), "contents", len(bt.contents), "child", str(bt.children), "contents", str(bt.contents)
    #         print list(bt)
    #         print l[:i+1]
    #         self.assertEqual( list(bt), l[:i + 1] )
    #
    #
    def test_bulkloads(self):
        bt = BTree.bulkload(range(50), 20)
        self.assertEqual(list(bt), range(50))
    #
    def test_removals(self):
        bt = BTree(20)
        l = range(30)
        map(bt.insert, l)
        rand = l[:]
        random.shuffle(rand)
        while l:
            self.assertEqual(list(bt), l)
            rem = rand.pop()
            print rem
            l.remove(rem)
            bt.remove(rem)
        self.assertEqual(list(bt), l)
    #
    # def test_insert_regression(self):
    #     bt = BTree.bulkload(range(50), 20)
    #
    #     for i in xrange(100):
    #         bt.insert(random.randrange(2000))



if __name__ == '__main__':
    unittest.main()
