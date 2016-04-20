import socket
import cloudpickle as pickle
import threading
import Queue

'''
This file contains helper functions for use in our parallelized 
implementations  of p_map(), p_filter(), and p_reduce()

Three of the methods in this file are prefixed by "_single_", indicating
that they will be used as helper functions for single chunks of data (as
opposed to large lists that comprise multiple chunks)
'''

DEFAULT_TIMEOUT = None #socket timeout
IP_ADDRESS = 'localhost' #run sockets on localhost

def _chunk_list(data, sz):
    '''
    Creates chunks of size `sz` from data. Returns a list of chunks, 
    which are also lists.

    These chunks will be sent to other machines on the network for processing.
    '''
    chunks = []
    for i in xrange(0, len(data), sz):
    	chunk = data[i:i + sz]
    	chunks.append(chunk)
    	# ideally, we could remove the chunked elements from the 
    	# list so we don't clog memory, but this messes up the loop
    	# data[i:i + sz] = []
    return chunks

def _single_map(foo, data):
	'''
	Map a function foo() over data (of type list). Map modifies data in place
	and supplies foo() with both the current element of the list and its
	respective index.

	This function is meant to be used on a chunk, which is a portion of 
	a list designated for a single machine. This function is called by 
	parallelogram.p_map()
	'''
	for index, elt in enumerate(data):
		data[index] = foo(elt, index)
	return data

def _single_filter(foo, data):
	'''
	Filter data (of type list) via a predicate formatted as a function. For 
	example, if data is a list of natural numbers from 1 to N:

		[1, 2, 3, ..., N]

	And you'd like to filter this list so that it contains only even numbers, 
	then simply define foo() to be:

		def foo(elt, index):
			return elt % 2 == 0

	This function is meant to be used on a chunk, which is a portion of 
	a list designated for a single machine. This function is called by 
	parallelogram.p_filter()
	'''
	for index, elt in enumerate(data):
		if not foo(elt, index):
			data.pop(index)
	return data

def _single_reduce(foo, data):
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

	This function is meant to be used on a chunk, which is a portion of 
	a list designated for a single machine. This function is called by 
	parallelogram.p_reduce()
	'''
	# as explained above, we need to apply foo() N-1 times
	for _ in range(len(data) - 1):
		data[0] = foo(data[0], data[1])
		# once the function has been applied, remove the old element
		data.pop(1)
	return data[0]

def _send_op(result, foo, chunk, op, index, port):
    dict_sending = {'func': foo, 'chunk': chunk, 'op': op, 'index': index}
    csts = threading.Thread(
        target = _client_socket_thread_send, 
        args = (port, pickle.dumps(dict_sending)))
    csts.start()
    queue = Queue.Queue()
    cstr = threading.Thread(
        target = _client_socket_thread_receive, 
        args = (port+1, queue))
    cstr.start()
    cstr.join(timeout = None)
    response = pickle.loads(queue.get())
    result[response['index']] = response['chunk']

def _server_socket_thread_send(target_port, msg):
    '''
    Starts a client thread to send a message to the target port
    :param target_port: The port to which the message should be sent
    :param msg: The message to send
    :return:
    '''
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_port = target_port
    clientsocket.connect((IP_ADDRESS, target_port))
    sent = clientsocket.send(msg)
    if sent == 0:
        raise RuntimeError("socket connection broken")
    clientsocket.close()

# based on examples from https://docs.python.org/2/howto/sockets.html
def _server_socket_thread_receive(port, queue):
    '''
    Starts a server socket that listens on the input port and writes 
    received messages to the queue. Has a blocking
    infinite loop, so should be run as a separate thread
    :param port: Port on which to listen for messages
    :param queue: Queue to add messages to
    '''
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((IP_ADDRESS, port))
    serversocket.listen(5)
    queue = queue
    while(True):
        clientsocket, _ = serversocket.accept()
        msg = clientsocket.recv(4096)
        if msg == '':
            raise RuntimeError("socket connection broken")
        queue.put(msg)
        clientsocket.close()

# based on examples from https://docs.python.org/2/howto/sockets.html
def _client_socket_thread_send(target_port, msg):
    '''
    Starts a client thread to send a message to the target port
    :param target_port: The port to which the message should be sent
    :param msg: The message to send
    :return:
    '''
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_port = target_port
    clientsocket.connect((IP_ADDRESS, target_port))
    sent = clientsocket.send(msg)
    if sent == 0:
        raise RuntimeError("socket connection broken")
    clientsocket.close()

    # based on examples from https://docs.python.org/2/howto/sockets.html
def _client_socket_thread_receive(port, queue):
    '''
    Starts a server socket that listens on the input port and writes 
    received messages to the queue. Has a blocking
    infinite loop, so should be run as a separate thread
    :param port: Port on which to listen for messages
    :param queue: Queue to add messages to
    '''
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind((IP_ADDRESS, port))
    serversocket.listen(5)
    clientsocket, _ = serversocket.accept()
    msg = clientsocket.recv(4096)
    if msg == '':
        raise RuntimeError("socket connection broken")
    queue.put(msg)
    clientsocket.close()