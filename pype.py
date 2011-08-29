import sys, operator, copy, re, random

from functools import *
from itertools import *

class Pype:
	def __init__(self, func, *args, **kwargs):
		"""the *args and **kwargs will get passed to func"""
		self.func = func
		self.args = args
		self.kwargs = kwargs

	def __call__(self, *args, **kwargs):
		"""lets Pype double as a PypeThunk"""
		return self.__class__(self.func, 
							*(self.args + args),
							**dict(self.kwargs.items() + kwargs.items()))
		    
	def __or__(self, rhs):
		"""combines two Pypes into a Chain"""
		return Chain(self, rhs)

class Map(Pype):
	def __ror__(self, lhs):
		return imap(lambda x:self.func(x, *self.args, **self.kwargs), lhs)

class Reduce(Pype):
	def __ror__(self, lhs):
		return reduce(self.func, lhs, *self.args)
	
class Sink(Pype):
	def __ror__(self, lhs):
		return self.func(lhs, *self.args, **self.kwargs)

class Filter(Pype):
	def __ror__(self, lhs):
		return ifilter(lambda x:self.func(x, *self.args, **self.kwargs), lhs)

class Chain(Pype):
	def __init__(self, lpype, rpype):
		self.lpype = lpype
		self.rpype = rpype

	def __ror__(self, lhs):
		return lhs | self.lpype | self.rpype
	

#FIXME: move everything below to pypelib



pDict = Sink(dict)
pSplit = Map(lambda s, *args, **kwargs:s.split(*args, **kwargs))
pReverse = Sink(lambda x:list(x)[::-1])
pSum = Sink(sum)
pWhile = lambda pred: Sink(lambda func:takewhile(pred, func))
pJoin = Sink(lambda ls, sep=' ': sep.join(ls))
pLen = Reduce(lambda x, y: x+1, 0)
pSet = Sink(set)
pStrip = Map(lambda s, *args, **kwargs:s.strip(*args, **kwargs))
pSort = Sink(sorted)
pValues = Sink(dict.itervalues)
pItems = Sink(dict.iteritems)
pList = Sink(list)
pSlice = Sink(islice)
pLower = Map(lambda s:s.lower())
pUpper = Map(lambda s:s.upper())
pTrue = Filter(lambda x: x)
pNonnull = Filter(lambda x: x is not None)

_nonce = object()
def _attr(obj, attr, default=_nonce):
	"""
	if obj has either an attr or a key attr, return that
	if neither and default is specified, return that
	else raise AttributeError
	"""
	try:
		return getattr(obj, attr)
	except AttributeError as e:
		if hasattr(obj, 'get'):
			val = obj.get(attr, default)
			if val != _nonce:
				return val
		elif default != _nonce:
			return default
		raise e

pAttr = Map(_attr)

# FIXME: use re.IGNORECASE; simply accept args that go into re.search
def _grep(line, word, invert=False, ignorecase=False):
	line = str(line)
	if ignorecase: 
		line = line.lower()
		word = word.lower()
	found = re.compile(word).search(line) is not None
	return invert ^ found

def _fgrep(line, words, invert=False, matchcase=True):
	line = str(line)
	if matchcase:
		line = line.lower()
		words = [w.lower() for w in words]
	found = reduce(operator.or_, (line.find(word) >= 0 for word in words))
	return invert ^ found

pGrep = Filter(_grep)
pFgrep = Filter(_fgrep)

def _flatten(iterable):
	"""flatten iterable, one level"""
	for items in iterable:
		for item in items:
			yield item

pFlatten = Sink(_flatten)

def _first(items):
	return iter(items).next()
pFirst = Sink(_first)

def _head(items, size=10):
	if size <= 0:
		raise StopIteration
	for cnt, item in enumerate(items):
		yield item
		if cnt == size-1:
			raise StopIteration

def _tail(items, last=10, skip=None):
	"""return either the last n items or all but the first n (skip) items
	if skip is not None then last will be ignored"""
	if skip is not None:
		for val in islice(items, skip, None):
			yield val
	else:
		buf = []
		for val in items:
			buf.append(val)
			if len(buf) > last:
				buf = buf[1:]
		for val in buf:
			yield val

pHead = Sink(_head)
pTail = Sink(_tail)

def _hist(values, mincount=0, topn=None):
	"""returns dict representing a histogram of a list of values
	only returns keys that appear at least mincount times
	only returns topn most frequent keys, all if topn is None"""
	hist = {}
	for v in values:
		hist[v] = hist.get(v, 0) + 1
	if mincount > 0:
		for key in hist.keys():
			if hist[key] < mincount:
				del hist[key]
	if topn is not None:
		hist = dict(sorted(hist.items(), key = lambda (k, v): -v)[:topn])
	return hist

pHist = Sink(_hist)

def _cut(lines, fields=0, delim=None, odelim=" ", suppress=False):
	"""a more sensible version of the GNU cut utility: cuts on whitespace by default"""
	for line in lines:
		words = line.split(delim)
		try:
			yield odelim.join([words[f] for f in fields]) if isinstance(fields, list) else words[fields]
		except IndexError:
			if not suppress: yield ""

pCut = Sink(_cut)

#FIXME: rewrite using itertools.groupby
def _uniq(values):
	"""removes consecutive duplicates, just like the UNIX command uniq"""
	first = True
	prev = None
	for v in values:
		sameasprev = v == prev
		first = False
		prev = v
		if first or not sameasprev: yield v

pUniq = Sink(_uniq)

"""streaming algorithms"""

def _sample(items, numsamples):
	samples = []
	for item, cnt in izip(items, count(1)):
		if random.random() < 1.0 * numsamples / cnt:
			if len(samples) < numsamples:
				samples.append(item)
			else:
				samples[random.randrange(len(samples))] = item
	return samples

pSample = Sink(_sample)

def _mean(items):
	mean = 0.0
	for cnt, item in enumerate(items):
		mean = (mean * cnt + item) / (cnt + 1)
	return mean

pMean = Sink(_mean)

def _writeToFile(values, filename_or_file, delim="\n"):
	if hasattr(filename_or_file, 'write'):
		file = filename_or_file
		close = lambda: None
	else:
		file = open(filename_or_file, 'w')
		close = lambda: file.close()
	for val in values:
		#FIXME: unicode?
		file.write(str(val))
		file.write(delim)
	close()

pWrite = Sink(_writeToFile)
pPrint = Sink(_writeToFile, sys.stdout)
pError = Sink(_writeToFile, sys.stderr)

"""export all pypes, and the Pype subclasses"""
__all__ = [w for w in dir() if w[0] == 'p'] + "Pype Map Reduce Sink Filter".split()
