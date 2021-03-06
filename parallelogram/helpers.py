'''
This file defines helper functions, methods, and classes for 
our implementations of p_map(), p_filter(), and p_reduce(), in addition
to our server implementation.

Included in this file:
2D array flattening (_flatten)
Code to chunk the data for sending over the network (_chunk_list)
Single machine versions of the parallelized functions for the server to run
on each chunk or the client to run on small enough datasets (single_map,
single_filter, single_reduce)
Matching algorithms to implement the uber model, one for the servers to
calculate their availability (calc_avaliability) which is included in
networking code, and one for the client to assign chunks based on this
(_get_chunk_assignments)
Remaining functions, labeled using the terms client/server,
socket/broadcast, and send/receive perform the described networking
function for the described entity


Three of the methods in this file are prefixed by "_single_", indicating
that they will be used as helper functions for single chunks of data (as
opposed to large lists that comprise multiple chunks)
'''
import Queue # allows machines to hold multiple chunks at one
import struct # helps us with object serialization and packing
import socket # allows for communication between machines
import threading # allows us to have multiple threads on clients/servers
import itertools # exposes helpful data manipulation methods
import psutil as ps # exposes system metrics for calcing availabilities  
import cloudpickle as pickle # allows for (de)serialization

# somtimes Python can't find the actual variables inside of config, 
# so it's safer to just assign variables this way
import config
DEFAULT_TIMEOUT = config.DEFAULT_TIMEOUT
MAX_CONNECT_REQUESTS = config.MAX_CONNECT_REQUESTS
NETWORK_CHUNK_SIZE = config.NETWORK_CHUNK_SIZE

def _flatten(multiarray):
    '''
    Flattens a 2D array into a 1D array using the itertools package. Ex:

    flatten([[1,2,3],[4,5,6],[7,8,9]]) = [1,2,3,4,5,6,7,8,9]

    :param multiarray: a 2D array to be flattened
    :return: a 1D representation of multiarray
    '''
    return list(itertools.chain.from_iterable(multiarray))

def _chunk_list(data, sz):
    '''
    Creates chunks of size `sz` from data. Returns a list of chunks, 
    which are also lists.

    These chunks will be sent to other machines on the network for processing.

    :param data: the data (of type list) to be split into chunks
    :param sz: the desired chunk size
    :return: a 2D array of chunks
    '''
    chunks = []
    for i in xrange(0, len(data), sz):
        chunk = data[i:i + sz]
        chunks.append(chunk)
    return chunks

def _single_map(foo, data):
    '''
    Map a function foo() over data (of type list). Map modifies data in place
    and supplies foo() with both the current element of the list and its
    respective index.

    This function is meant to be used on a chunk, which is a portion of 
    a list designated for a single machine. This function is called by 
    parallelogram.p_map()

    :param foo: the function to map over data
    :param data: the data to be mapped
    :return: the mapped data
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

    :param foo: the function to filter over data
    :param data: the data to be filtered
    :return: the filtered data
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

    :param foo: the function to reduce over data
    :param data: the data to be reduced
    :return: a single reduction value
    '''
    # ensure that data actually exists
    assert(len(data) > 0)

    # as explained above, we need to apply foo() N-1 times
    for _ in range(len(data) - 1):
        data[0] = foo(data[0], data[1])
        # once the function has been applied, remove the old element
        data.pop(1)
    return data[0]

def _send_op(result, foo, chunk, op, index, target_ip, own_ip, port, timeout):
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
            args = (target_ip, port, pickle.dumps(dict_sending), timeout))
        csts.start()
        queue = Queue.Queue()
        cstr = threading.Thread(
            target = _client_socket_thread_receive,
            args = (own_ip, port+1, queue, timeout))
        cstr.start()
        cstr.join(timeout = None)
        response = pickle.loads(queue.get())
        result[response['index']] = response['chunk']
    except RuntimeError, socket.timeout:
        return #do nothing on error, just end and the client will restart the sending protocol

# based on examples from https://docs.python.org/2/howto/sockets.html
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

# based on examples from https://docs.python.org/2/howto/sockets.html
class _Server_Socket_Thread_Receive(threading.Thread):
    def __init__(self, ip, port, queue):
        '''
        Starts a server socket that listens on the input port and writes
        received messages to the queue. Has a blocking
        infinite loop, so should be run as a separate thread
        A class rather than a function to make it stoppable and allow
        cleaner socket closing in infinite loop

        :param port: Port on which to listen for messages
        :param queue: Queue to add messages to
        '''
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
        '''
        while not self._abort:
            try:
                clientsocket, address = self.serversocket.accept()
            except socket.timeout:
                #reset blocking client on timeout
                continue
            msg = clientsocket.recv(NETWORK_CHUNK_SIZE)
            if msg == '':
                raise RuntimeError("Socket connection broken!")
            self.queue.put((msg, address[0]))
            clientsocket.close()

    def stop(self):
        '''
        stop server and nicely close sockets
        '''
        self._abort = True

# based on examples from https://docs.python.org/2/howto/sockets.html
def _client_socket_thread_send(target_ip, target_port, msg, timeout):
    '''
    Starts a client thread to send a message to the target port

    :param target_ip: The ip address to which the message should be sent
    :param target_port: The port to which the message should be sent
    :param msg: The message to send
    :return: how long in seconds to wait before giving up on sending
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

    :param ip: The ip address on which to listen
    :param port: Port on which to listen for messages
    :param queue: Queue to add messages to
    :param timeout: how long in seconds to wait before giving up on receiving
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
    Function run by client that broadcasts that there is a job it wants servers
    to perform, and returns a list of tuples of avaliable servers and how
    'avaliable' and willing to take new chunks each of them are

    :param mult_group_ip: multicast group ip address on which to broadcast
    :param mult_port: multicast port to send to
    :param server_list: empty list to add to avaliable server/server avaliability metric tuples
    '''
    message = str('job')
    multicast_group = (mult_group_ip, mult_port)

    # Create the datagram socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Set a timeout so the socket does not block indefinitely when trying
    # to receive data.
    sock.settimeout(2)

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
class _Broadcast_Server_Thread(threading.Thread):
    def __init__(self, mult_group_ip, mult_port, chunk_queue):
        '''
        Function run by server to listen on the multicast channel for new
        clients, and to send back that the machine is avaliable and how
        avaliable/busy it is. Implemented as a class since it listen forever
        until the server is stopped, at which time it should exit nicely

        :param mult_group_ip: multicast group ip address on which to broadcast
        :param mult_port: multicast port to send to
        :param chunk_queue: queue of chunks that are waiting to be processed
        '''
        self.chunk_queue = chunk_queue
        self._abort = False
        threading.Thread.__init__(self)

        server_address = ('', mult_port)

        # Create the socket and bind it to the server address
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(server_address)

        # Tell the operating system to add the socket to the multicast group
        # on all interfaces.
        group = socket.inet_aton(mult_group_ip)
        mreq = struct.pack('4sL', group, socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    def run(self):
        '''
        starts receive and respond loop
        '''
        while not self._abort:
            msg, address = self.sock.recvfrom(NETWORK_CHUNK_SIZE)
            avaliability = self.calc_avaliability()
            if msg == 'job':
                self.sock.sendto(str(avaliability), address)
        self.sock.close()

    def calc_avaliability(self):
        '''
        Function to calculate avaliability of given machine. Change this
        function to customize the metric for your application/network
        :return:
        '''
        avaliability = self.chunk_queue.qsize()
        return avaliability
        '''
        To use system cpu percentage as a metric swap in the following code:
        avaliability = ps.cpu_percent(interval=.5) 
        return avaliability
        '''
        '''
        To use available memory as a metric swap in the following code:
        #ps.virtual_memory()[2] returns available system memory as a 
        #percentage. We return 100 minus this value to keep consistency
        #with smaller availability scores corresponding to freer machines.
        avaliability = 100 - ps.virtual_memory()[2] 
        return avaliability
        '''
    def stop(self):
        '''
        stop server and nicely close sockets
        '''
        self._abort = True
#naive implementation: give each chunk to minimum avaliability server, then increment
#avaliability of that server modify this function to change how chunks are
#assigned
def _get_chunk_assignments(avaliable_servers, num_chunks):
    '''
    Given a certain number of chunks, assigns each of them to a server
    based on the number of servers who identified themselves and what
    each of their avaliabilities are

    :param avaliable_servers: list of tuples with ip address of avaliable
           servers and each of their avaliability metrics
    :param num_chunks: number of chunks to assign
    :return: list of len(num_chunks) in which each index is the ip address
             of the server to send the given chunk to
    '''
    zipped_avaliable_servers = zip(*avaliable_servers)

    # raise an AssertionError if there are no servers avaialable
    assert(len(zipped_avaliable_servers) != 0)
    
    server_list = list(zipped_avaliable_servers[0])
    avaliability_list = list(zipped_avaliable_servers[1])

    chunk_address_list = list()
    for i in xrange(0, num_chunks):
        # grab the server processing the least number of chunks
        min_available = avaliability_list.index(min(avaliability_list))
        chunk_address_list.append(server_list[min_available])
        avaliability_list[min_available] =  avaliability_list[min_available] + 1
    return chunk_address_list
