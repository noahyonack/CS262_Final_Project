''' 
This file contains our library's implementations of map(), filter(), and 
reduce().
'''

def map(foo, data):
	'''
	Map a function foo() over data (of type list). Map modifies data in place
	and supplies foo() with both the current element of the list and its
	respective index.
	'''
	for index, elt in enumerate(data):
		data[index] = foo(elt, index)
	return data

def filter(foo, data):
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

def reduce(foo, data):
	'''
	Reduce data (of type list) by continually applying foo() to subsequent
	elements of data. For exmaple, if data is a list of elements e_1 to e_N:

		[e_1, e_2, e_3, ..., e_N]

	And you'd like to reduce this list to the sum of all the elements, then 
	simply define foo() to be:

		def foo(elt1, elt2):
			return elt1 + elt2

	foo() is applied to data in the following way:

		1. [foo(e_1, e_2), e_3, ..., e_N]
		2. [foo(foo(e_1, e_2), e_3), ..., e_N]
		...
		N - 1. [foo(foo(foo(e_1, e_2), e_3), ..., e_N)]
	'''
	# as explained above, we need to apply foo() N-1 times
	for _ in range(len(data) - 1):
		data[0] = foo(data[0], data[1])
		# once the function has been applied, remove the old element
		data.pop(1)
	return data[0]







