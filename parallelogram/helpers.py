import socket
import cloudpickle as pickle
import threading
import itertools
import Queue
import struct
import sys

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

def flatten(multiarray):
    '''
    Flattens a 2D array into a 1D array using the itertools package. Ex:

    flatten([[1,2,3],[4,5,6],[7,8,9]]) = [1,2,3,4,5,6,7,8,9]
    '''
    return list(itertools.chain.from_iterable(multiarray))

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
    then simply define foo(elt, index) to be:

        return elt % 2 == 0

    This function is meant to be used on a chunk, which is a portion of 
    a list designated for a single machine. This function is called by 
    parallelogram.p_filter()
    '''
    for index, elt in reversed(list(enumerate(data))):
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

    Note: this function assumes non-empty data. An exception will be thrown
    if an empty list is passed in.
    '''
    # ensure that data actually exists
    # assert(len(data) > 0)

    # as explained above, we need to apply foo() N-1 times
    for _ in range(len(data) - 1):
        data[0] = foo(data[0], data[1])
        # once the function has been applied, remove the old element
        data.pop(1)
    return data[0]

def _send_op(result, foo, chunk, op, index, ip, port, timeout):
    '''
    Sends an operation over the network for a server to process, and 
    receives the result. Since we want each chunk to be sent in 
    parallel, this should be threaded

    :param result: empty list passed by reference which will contain the result. 
        Necessary because threads don't allow standard return values
    :param foo: function to use for map, filter, or reduce calls
    :param chunk: chunk to perform operation on
    :param op: string corresponding to operation to perform: 
        'map', 'filter', 'reduce'
    :param index: chunk number to allow ordering of processed chunks
    :param port: port of server
    '''
    try:
        dict_sending = {'func': foo, 'chunk': chunk, 'op': op, 'index': index}
        csts = threading.Thread(
            target = _client_socket_thread_send,
            args = (ip, port, pickle.dumps(dict_sending), timeout))
        csts.start()
        queue = Queue.Queue()
        cstr = threading.Thread(
            target = _client_socket_thread_receive,
            args = (ip, port+1, queue, timeout))
        cstr.start()
        cstr.join(timeout = None)
        response = pickle.loads(queue.get())
        result[response['index']] = response['chunk']
    except RuntimeError, socket.timeout:
        return #do nothing on error, just end and the client will restart the sending protocol

def _server_socket_thread_send(ip, target_port, msg):
    '''
    Starts a server thread to send a message to the target port
    
    :param target_port: The port to which the message should be sent
    :param msg: The message to send
    '''
    socket.setdefaulttimeout(DEFAULT_TIMEOUT)
    #defines socket as internet, streaming socket
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    target_port = target_port
    clientsocket.connect((ip, target_port))
    sent = clientsocket.send(msg)
    if sent == 0:
        raise RuntimeError("Socket connection broken!")
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
    def __init__(self, ip, port, queue):
        #socket setup
        socket.setdefaulttimeout(DEFAULT_TIMEOUT)
        #defines socket as internet, streaming socket
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #prevents socket waiting for additional packets 
        #after end of channel to allow quick reuse
        self.serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        #bind socket to given ip address and port
        self.serversocket.bind((ip, port))
        #allows up to MAX_CONNECT_REQUESTS requests 
        #before refusing outside connections
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
                raise RuntimeError("Socket connection broken!")
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
def _client_socket_thread_send(target_ip, target_port, msg, timeout):
    '''
    Starts a client thread to send a message to the target port

    :param target_port: The port to which the message should be sent
    :param msg: The message to send
    :return:
    '''
    #socket setup
    socket.setdefaulttimeout(timeout)
    #defines socket as internet, streaming socket
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #connects to given ip address and port
    clientsocket.connect((target_ip, target_port))
    try:
        sent = clientsocket.send(msg)
        if sent == 0:
            raise RuntimeError("Socket connection broken!")
    finally:
        clientsocket.close()

# based on examples from https://docs.python.org/2/howto/sockets.html
def _client_socket_thread_receive(ip, port, queue, timeout):
    '''
    Starts a client socket that listens on the input port and writes
    received messages to the queue. Is blocking, so should be run on a 
    separate thread
    
    :param port: Port on which to listen for messages
    :param queue: Queue to add messages to
    '''
    #socket setup
    socket.setdefaulttimeout(timeout)
    #defines socket as internet, streaming socket
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #prevents socket waiting for additional packets after end of 
    #channel to allow quick reuse
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #bind socket to given ip address and port
    serversocket.bind((ip, port))
    #allows up to MAX_CONNECT_REQUESTS requests before 
    # refusing outside connections
    serversocket.listen(MAX_CONNECT_REQUESTS)
    #want to make sure to close clientsocket on timeout, which throws a socket.timeout exception
    clientsocket, _ = serversocket.accept()
    try:
        msg = clientsocket.recv(NETWORK_CHUNK_SIZE)
        if msg == '':
            raise RuntimeError("Socket connection broken!")
        queue.put(msg)
    finally:
        clientsocket.close()

#based on sample code from https://pymotw.com/2/socket/multicast.html
def _broadcast_client_thread(mult_group_ip, mult_port, server_list):
    '''

    :param mult_group_ip:
    :param mult_port:
    :param queue:
    :return:
    '''
    message = str('job')
    multicast_group = (mult_group_ip, mult_port)

    # Create the datagram socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Set a timeout so the socket does not block indefinitely when trying
    # to receive data.
    sock.settimeout(0.2)

    # Standard time-to-live value scopes:
    # 0    Restricted to the same host
    # 1    Restricted to the same subnet
    # <32  Restricted to the same site
    # <64  Restricted to the same region
    # <128 Restricted to the same continent
    # <255 Global
    ttl = struct.pack('b', 1)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

    try:
        # Send data to the multicast group
        sock.sendto(message, multicast_group)


        # Look for responses from all recipients
        while True:
            try:
                avaliability, address = sock.recvfrom(NETWORK_CHUNK_SIZE)
            except socket.timeout:
                break
            else:
                #only want ip address from address tuple, not port
                server_list.append((address[0], int(avaliability)))

    finally:
        sock.close()

#based on sample code from https://pymotw.com/2/socket/multicast.html
class Broadcast_Server_Thread(threading.Thread):
    def __init__(self, mult_group_ip, mult_port, chunk_queue):
        '''

        :param mult_group_ip:
        :param mult_port:
        :param avaliability:
        :return:
        '''
        self.chunk_queue = chunk_queue
        self._abort = False
        threading.Thread.__init__(self)

        server_address = ('', mult_port)

        # Create the socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Bind to the server address
        self.sock.bind(server_address)

        # Tell the operating system to add the socket to the multicast group
        # on all interfaces.
        group = socket.inet_aton(mult_group_ip)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def run(self):
        # Receive/respond loop
        while not self._abort:
            msg, address = self.sock.recvfrom(NETWORK_CHUNK_SIZE)
            avaliability = self.calc_avaliability()
            if msg == 'job':
                self.sock.sendto(str(avaliability), address)
        self.sock.close()

    def calc_avaliability(self):
        avaliability = self.chunk_queue.qsize()
        return avaliability

    def stop(self):
        self._abort = True


#first attempt: give each chunk to minimum avaliability server, then increment avaliability of that server
#modify this function to change how chunks are assigned
#todo: maybe this should pass in a function? Then can send assignment algorithm from top level instead of editing this function?
def get_chunk_assignments(avaliable_servers, num_chunks):
    '''

    :param avaliable_servers:
    :param num_chunks:
    :return:
    '''

    zipped_avaliable_servers = zip(*avaliable_servers)
    server_list = list(zipped_avaliable_servers[0])
    avaliability_list = list(zipped_avaliable_servers[1])
    chunk_address_list = list()
    for i in xrange(0,num_chunks):
        min_avaliable = avaliability_list.index(min(avaliability_list))
        chunk_address_list.append(server_list[min_avaliable])
        avaliability_list[min_avaliable] =  avaliability_list[min_avaliable] + 1
    return chunk_address_list