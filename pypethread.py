"""
Threaded pypes!

Usage is very simple: use MapThreaded and FilterThreaded in place
of Map and Thread to get parallelization for free. 
"""

from __future__ import with_statement

import threading, time, utils, random, sys

from itertools import *
from functools import *

from pype import *

class WorkerException(Exception):
	pass

class Worker(threading.Thread):
	def __init__(self, input, outbuf, buflock, func, args, kwargs):
		"""input: iterator
		outbuf: dict where outputs will be written to
		buflock: condition for writing to outbuf as well as signalling we're done
		func, args, kwargs: function and arguments to execute"""
		threading.Thread.__init__(self)

		#isAlive() doesn't quite do it, because we can get interrupted
		#between calling notify() and actually dying
		self.finished = False

		#automatically copy args to self
		for k, v in locals().iteritems():
			if k != "self":
				setattr(self, k, v)
	
	def run(self):
		input = iter(self.input)
		while True:
			#FIXME: errors in input are ignored silently
			id = None
			try:
				id, arg = input.next()
				val = self.func(arg, *self.args, **self.kwargs)
			except StopIteration:
				break
			except Exception, error:
				#save the exception so the consumer can re-raise it
				val = WorkerException((error, sys.exc_info()[2]))
				break
			with self.buflock:
				if id is not None:
					self.outbuf[id] = (arg, val)
				self.buflock.notify()
		with self.buflock:
			self.finished = True
			self.buflock.notify()

class PypeThreaded(Pype):
	"""base class for threaded pype classes"""
	def __init__(self, func, *args, **kwargs):
		self.func = func
		self.args = args
		self.kwargs = kwargs
		
		#this is slightly ugly: _max_threads and _buf_len are both 
		#shared with the function arguments
		#the alternative is to accept an array and dict respy. for args and kwargs
		#but that's more inconvenient for the caller, so ugly it is
		self.maxthreads = 8
		if '_max_threads' in kwargs:
			self.maxthreads = int(kwargs['_max_threads'])
			del kwargs['_max_threads']

		#a thread will block for lack of buffer space if it takes more than 3 times
		#as much time on an arg as the average time taken by other threads.
		#which is pretty unlikely.
		self.buflen = 3 * self.maxthreads
		if '_buf_len' in kwargs:
			self.buflen = int(kwargs['_buf_len'])
			del kwargs['_buf_len']

	def processBufElem(self, arg, val):
		"""this needs to be overridden by children
		arg and val are the input and output of the function
		returns a 2-tuple, the first indicating whether or not to yield this value,
		and the second being the actual value to yield
		
		this function encapsulates the difference in behavior between Map and Filter"""
		pass

	def __ror__(self, lhs):
		outbuf = {}
		buflock = threading.Condition()
		bufspace = threading.BoundedSemaphore(self.buflen)
		input = Producer(izip(count(), lhs), bufspace)
		threads = [Worker(input, outbuf, buflock, 
							self.func, self.args, self.kwargs)\
							for spam in xrange(self.maxthreads)]
		for thread in threads:
			thread.start()

		#nextid is the id of the next value in the output stream that we're waiting on
		nextid = 0

		while True:
			with buflock:
				#look for values in output buffer
				while nextid in outbuf:
					arg, possibleexception = outbuf[nextid]
					if isinstance(possibleexception, WorkerException):
						#the exception in the thread
						error, tb = possibleexception.message
						raise error, None, tb
					yld, val = self.processBufElem(*outbuf[nextid])
					if yld:
						yield val
					del outbuf[nextid]
					bufspace.release()
					nextid += 1

				#check if threads are done
				if False not in [thread.finished for thread in threads]:
					assert len(outbuf) == 0
					break

				#wait on worker threads
				buflock.wait()

class FilterThreaded(PypeThreaded):
	def processBufElem(self, arg, val):
		return (val, arg)

class MapThreaded(PypeThreaded):
	def processBufElem(self, arg, val):
		return (True, val)

class Producer:
	def __init__(self, iterator, bufspace):
		self.iterator = iterator
		self.bufspace = bufspace
		self.nextlock = threading.Lock()

	def __iter__(self):
		return self

	def next(self):
		#FIXME: test what happens when iterator craps out
		self.bufspace.acquire()
		try:
			with self.nextlock:
				return self.iterator.next()
		except StopIteration:
			self.bufspace.release()
			raise StopIteration
		except:
			self.bufspace.release()
			raise
			

MapThreaded, FilterThreaded = Map, Filter
__all__ = [w + "Threaded" for w in "Pype Map Filter".split()]

if __name__ == "__main__":
	xrange(20) | FilterThreaded(lambda x:x%2) | pPrint
