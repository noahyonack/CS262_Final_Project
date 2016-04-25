def _single_filter(foo, data):
	for index, elt in reversed(list(enumerate(data))):
		if not foo(elt, index):
			data.pop(index)
	return data

def foo(elt, index):
	return elt == 2 

big_list = range(10000)
print _single_filter(foo, big_list)
