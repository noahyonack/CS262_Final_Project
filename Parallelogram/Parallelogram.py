''' 
This file contains our library's implementations of map(), filter(), and 
reduce().
'''

def map(data, foo):
	for index, elt in enumerate(data):
		data[index] = foo(elt)
	return data