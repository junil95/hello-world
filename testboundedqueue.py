import unittest
from boundedqueue import BoundedQueue

class TestBoundedQueue(unittest.TestCase):
    '''Test all functions of BoundedQueue'''
    def setUp(self):
        self.boundedqueue = BoundedQueue(10)

    def test_half_capacity_bounded_queue(self):
        """Tests if the queue and dequeue functions work"""
        for i in range(5,10):
            self.boundedqueue.enqueue(i)
        self.assertEqual(self.boundedqueue.dequeue(),5)
        self.assertEqual(self.boundedqueue.dequeue(),6)
        self.assertEqual(self.boundedqueue.dequeue(),7)
        self.assertEqual(self.boundedqueue.dequeue(),8)
        self.assertEqual(self.boundedqueue.dequeue(),9)
        self.assertEqual(self.boundedqueue.dequeue(),10) 
        
    def test_full_capacity_enqueue(self):
        """Tests to see if anything changes when more elements then capacity are enqueued"""
        for i in range(0,10):
            self.boundedqueue.enqueue(i)
        #add an extra element past capacity but nothing should change
        self.boundedqueue.enqueue(11)
        #dequeue all elements and see if only 1-10 return and not 11
        #making sure the 11 didnt replace any elements already in the queue
        self.assertEqual(self.boundedqueue.dequeue(),1)
        self.assertEqual(self.boundedqueue.dequeue(),2)
        self.assertEqual(self.boundedqueue.dequeue(),3)
        self.assertEqual(self.boundedqueue.dequeue(),4)
        self.assertEqual(self.boundedqueue.dequeue(),5)
        self.assertEqual(self.boundedqueue.dequeue(),6)
        self.assertEqual(self.boundedqueue.dequeue(),7)
        self.assertEqual(self.boundedqueue.dequeue(),8)
        self.assertEqual(self.boundedqueue.dequeue(),9)
        self.assertEqual(self.boundedqueue.dequeue(),10)      
    
    def test_empty_dequeue(self):
        """Test to see if IndexError is raised when user dequeues an empty list"""
        self.assertRaises(IndexError,self.boundedqueue.dequeue())
    def test_empty_queue(self):
        """Test if a empty bounded queue returns True when calling the function is_empty"""
        self.assertTrue(self.boundedqueue.is_empty())
        
    def test_non_empty_queue(self):
        """Test if a non empty bounded queue returns False when calling function is_empty"""
        for i in range(5,10):
            self.boundedqueue.enqueue(i)
        self.assertFalse(self.boundedqueue.is_empty())
        
    def test_empty_bounded_queue_size(self):
        """Tests if the correct size is returned for a empty boundedqueue"""
        #empty bounded queue       
        self.assertEquals(self.boundedqueue.size(),0)
        
    def test_non_empty_bounded_queue_size(self):
        """Tests if the correct size is returned for a non empty boundedqueue"""
        #non empty bounded queue
        for i in range(5,10):
            self.boundedqueue.enqueue(i)
        self.asserEquals(self.boundedqueue.size(),5)
        #check if size decreased by 1
        self.boundedqueue.dequeue()
        self.assertEquals(self.boundedqueue.size(),4)
    
    def test_fifo_for_bounded_queue(self):
        """Test if FIFO is working properly. So if i add values while there are still values in the queue then it adds the new values at the end and removes the old values first"""
        for i in range(1,6):
            self.boundedqueue.enqueue(i)
        self.assertEqual(self.boundedqueue.dequeue(),1)
        self.assertEqual(self.boundedqueue.dequeue(),2)
        #add more values 
        for i in range(40,42):
            self.boundedqueue.enqueue(i)
        self.assertEqual(self.boundedqueue.dequeue(),3)
        self.assertEqual(self.boundedqueue.dequeue(),4)
        self.assertEqual(self.boundedqueue.dequeue(),5)
        self.assertEqual(self.boundedqueue.dequeue(),6)
        self.assertEqual(self.boundedqueue.dequeue(),40)
        
        
if __name__ == '__main__':
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestBoundedQueue)
    unittest.TextTestRunner(verbosity=2).run(test_suite)      