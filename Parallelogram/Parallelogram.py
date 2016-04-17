''' 
This file contains our library's implementations of map(), filter(), and 
reduce().
'''

def map(data, foo):
	'''
	Map a function foo() over data (of type list). Map modifies data in place
	and supplies foo() with both the current element of the list and its
	respective index.
	'''
	for index, elt in enumerate(data):
		data[index] = foo(elt, index)
	return data

def filter(data, foo):
	'''
	Filter data (of type list) via a predicate formatted as a function. For 
	example, if data is a list of natural numbers from 1 to N:

		[1, 2, 3, ..., N]

	And you'd like to filter this list so that it contains only even numbers, 
	then simply define foo() to be:

		def foo(elt, index):
			return elt % 2 == 0
	'''
	for index, elt in enumerate(data):
		if not foo(elt, index):
			data.pop(index)
	return data