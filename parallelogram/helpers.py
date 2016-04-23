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

DEFAULT_TIMEOUT = 5 #socket timeout
IP_ADDRESS = 'localhost' #run sockets on localhost
#replace with socket.gethostname() for external access
MAX_CONNECT_REQUESTS = 5 #max queue for socket requests
NETWORK_CHUNK_SIZE = 8192 #max buffer size to read

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
    '''
    Sends an operation over the network for a server to process, and 
    receives the result. Since we want each chunk to be sent in 
    parallel, this should be threaded

    :param result: empty list passed by reference which will contain the result. 
        Necessary because threads don't allow standard return values
    :param foo: function to use for map, filter, or reduce
    :param chunk: chunk to perform operation on
    :param op: string corresponding to operation to perform: 
        'map', 'filter', 'reduce'
    :param index: chunk number to allow ordering of processed chunks
    :param port: port of server
    '''
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
    '''
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_port = target_port
    clientsocket.connect((IP_ADDRESS, target_port))
    sent = clientsocket.send(msg)
    if sent == 0:
        raise RuntimeError("socket connection broken")
    clientsocket.close()

class _Server_Socket_Thread_Receive(threading.Thread):
    '''
    Starts a server socket that listens on the input port and writes
    received messages to the queue. Has a blocking
    infinite loop, so should be run as a separate thread
    A class rather than a function to make it stopable and allow 
    cleaner socket closing in infinite loop
    
    :param port: Port on which to listen for messages
    :param queue: Queue to add messages to
    '''
    def __init__(self, port, queue):
        #socket setup
        socket.setdefaulttimeout(DEFAULT_TIMEOUT)
        #defines socket as internet, streaming socket
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #prevents socket waiting for additional packets after end of channel to allow quick reuse
        self.serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #bind socket to given ip address and port
        self.serversocket.bind((IP_ADDRESS, port))
        #allows up to MAX_CONNECT_REQUESTS requests before refusing outside connections
        self.serversocket.listen(MAX_CONNECT_REQUESTS)
        self.queue = queue
        self._abort = False
        threading.Thread.__init__(self)

    def run(self):
        '''
        Loop that listens for messages and processes them if they arrive, 
        adding them to the queue

        :return:
        '''
        while not self._abort:
            try:
                clientsocket, _ = self.serversocket.accept()
            except socket.timeout:
                #reset blocking client on timeout
                continue
            msg = clientsocket.recv(NETWORK_CHUNK_SIZE)
            if msg == '':
                raise RuntimeError("socket connection broken")
            self.queue.put(msg)
            clientsocket.close()

    def stop(self):
        self._abort = True

# # based on examples from https://docs.python.org/2/howto/sockets.html
# def _server_socket_thread_receive(port, queue):
#     '''
#     Starts a server socket that listens on the input port and writes
#     received messages to the queue. Has a blocking
#     infinite loop, so should be run as a separate thread
#     :param port: Port on which to listen for messages
#     :param queue: Queue to add messages to
#     '''
#     socket.setdefaulttimeout(DEFAULT_TIMEOUT)
#     serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     serversocket.bind((IP_ADDRESS, port))
#     serversocket.listen(5)
#     queue = queue
#     while(True):
#         clientsocket, _ = serversocket.accept()
#         msg = clientsocket.recv(NETWORK_CHUNK_SIZE)
#         if msg == '':
#             raise RuntimeError("socket connection broken")
#         queue.put(msg)
#         clientsocket.close()


# based on examples from https://docs.python.org/2/howto/sockets.html
def _client_socket_thread_send(target_port, msg):
    '''
    Starts a client thread to send a message to the target port
    :param target_port: The port to which the message should be sent
    :param msg: The message to send
    :return:
    '''
    #socket setup
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    #defines socket as internet, streaming socket
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_port = target_port
    #connects to given ip address and port
    clientsocket.connect((IP_ADDRESS, target_port))
    sent = clientsocket.send(msg)
    if sent == 0:
        raise RuntimeError("socket connection broken")
    clientsocket.close()

    # based on examples from https://docs.python.org/2/howto/sockets.html
def _client_socket_thread_receive(port, queue):
    '''
    Starts a server socket that listens on the input port and writes
    received messages to the queue. Is blocking, so should be run on a 
    separate thread
    
    :param port: Port on which to listen for messages
    :param queue: Queue to add messages to
    '''
    #socket setup
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    #defines socket as internet, streaming socket
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #prevents socket waiting for additional packets after end of 
    # channel to allow quick reuse
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #bind socket to given ip address and port
    serversocket.bind((IP_ADDRESS, port))
    #allows up to MAX_CONNECT_REQUESTS requests before 
    # refusing outside connections
    serversocket.listen(MAX_CONNECT_REQUESTS)
    clientsocket, _ = serversocket.accept()
    msg = clientsocket.recv(NETWORK_CHUNK_SIZE)
    if msg == '':
        raise RuntimeError("socket connection broken")
    queue.put(msg)
    clientsocket.close()