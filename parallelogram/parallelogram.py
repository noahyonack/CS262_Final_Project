''' 
This file contains our library's implementations of p_map(), p_filter(), and 
p_reduce().

We use the letter "p" because it indicates that the method is paralellized
and because doing so ensures that our functions are properly namespaced

Imports helpers.py, which contains 'private' helper functions
'''

import helpers
import threading
import Queue
import socket

# from config import MULTICAST_PORT, MULTICAST_GROUP_IP
import config
MULTICAST_PORT = config.MULTICAST_PORT
MULTICAST_GROUP_IP = config.MULTICAST_GROUP_IP

'''
Our minimum chunk size in terms of number of elements. When asked to map(),
filter(), or reduce() a presumably large list, we chunk it in pieces of 
CHUNK_SIZE and send those chunks to other physical machines on the network.

If the list on which to apply a function foo() is smaller than CHUNK_SIZE, 
it makes more sense to have the calling machine process the chunk instead
of sending it over the wire.
'''
CHUNK_SIZE = 6
# IP_ADDRESS = 'localhost' #run sockets on localhost
# gets ip address of machine on network
IP_ADDRESS = socket.gethostbyname(socket.gethostname())

def p_map(foo, data, port, timeout):
    '''
    Map a function foo() over chunks of data (of type list) and
    join the mapped chunks before returning back to the caller.

    This mapping will likely not be done on a single machine (unless the data
    to be mapped over is so small that sending it over a network would be
    inefficient.)

    :param foo: function to map over data
    :param data: a list of data to be mapped over
    :param port: a port by which to send over distributed operations
    :param timeout: timeout, in seconds, that function should wait for chunks to be returned
    :return: the mapped results
    '''
    try:
        result = p_func(foo, data, port, 'map', timeout)
        return helpers._flatten(result)
    except RuntimeError:
        # if no servers are available, run the job yourself
        return helpers._single_map(foo, data)

def p_filter(foo, data, port, timeout):
    '''
    Filter a function foo() over chunks of data (of type list) and
    join the filtered chunks before returning back to the caller.

	This filtering will likely not be done on a single machine (unless the data
	to be filtered over is so small that sending it over a network would be
	inefficient.)

	:param foo: function to filter over data
    :param data: a list of data to be filtered
    :param port: a port by which to send over distributed operations
    :return: the filtered results
	'''
    try:
        result = p_func(foo, data, port, 'filter', timeout)
        return helpers._flatten(result)
    except RuntimeError:
        # if no servers are available, run the job yourself
        return helpers._single_filter(foo, data)

def p_reduce(foo, data, port, timeout):
    '''
    Reduce a function foo() over chunks of data (of type list) and
	then reduce the results before returning back to the caller.

	This reduction will likely not be done on a single machine (unless the data
	to be reduced over is so small that sending it over a network would be
	inefficient.)

	After the intial chunks have been reduced, we still need to reduce
	the results of the initial reduction, so we call our function again
	and either redistribute the initial results or simply locally-process
	chunks, depending on the size of the initial results array.
    
    :param foo: function to reduce over data
    :param data: a list of data to be reduced
    :param port: a port by which to send over distributed operations
    :param timeout: timeout, in seconds, that function should wait for chunks to be returned
    :return: the reduced result (a single value!)
    '''
    # ensure that data is present
    assert(len(data) > 0)

    try:
        result = p_func(foo, data, port, 'reduce', timeout)

        # checks if single value is returned, which means reduce is done.
        # this case will only occur when our initial data is <= than CHUNK_SIZE
        if (len(result) == 1):
            return result[0]
        else:
            result = helpers._flatten(result)
            # if we have less than CHUNK_SIZE elements, just locally compute.
            # otherwise, call a new round of distributed reduction!
            if (len(result) <= CHUNK_SIZE):
                return helpers._single_reduce(foo, result)
            else:
                return p_reduce(foo, result, port, timeout)
    except RuntimeError:
        # if no servers are available, run the job yourself
        return helpers._single_reduce(foo, data)

def p_func(foo, data, port, op, timeout):
    '''
    Performs network operations for parallel map, filter, and reduce functions

    :param foo: function to reduce over data
    :param data: a list of data to be reduced
    :param port: a port by which to send over distributed operations
    :param op: operation to perform, can be 'map', 'reduce', or 'filter
    :param timeout: timeout, in seconds, that function should wait for chunks to be returned
    :return:
    '''
    # get list of avaliable servers to send to
    # can block since we need list of machines to continue, don't need to thread
    available_servers = list()
    helpers._broadcast_client_thread(MULTICAST_GROUP_IP, MULTICAST_PORT, available_servers)

    # chunk the data so it can be sent out in pieces
    chunks = helpers._chunk_list(data, CHUNK_SIZE)

    try:
        #list of length len(chunks) with the address to send each chunk to
        chunk_assignments = helpers._get_chunk_assignments(available_servers, len(chunks))
    except AssertionError:
        raise RuntimeError("There aren't any available servers on the network!")

    # placeholder for data to be read into
    result = [None] * len(chunks)
    # list of threads corresponding to sent chunks
    compute_threads = [None] * len(chunks)
    #iterate while some chunks are still None, and thus have not been completed
    while None in result:
        for index, chunk in enumerate(chunks):
            #don't resend completed chunks, move on to next result element
            if result[index] != None:
                continue
            # spawns separate thread to distribute each chunk and collect results
            compute_threads[index] = threading.Thread(
                target = helpers._send_op,
                args = (result, foo, chunk, op, index, chunk_assignments[index], IP_ADDRESS, port, timeout))
            compute_threads[index].start()
            # ideally, we'd like to pop the chunk after processing
            # it to preserve memory, but this messes up the loop
            # chunks.pop(index)
        # wait for all threads to finish, so we know all results are in
        for thread in compute_threads:
            thread.join()
        # doubles timeout in case chunk just took a really long time to process, preventing the case where
        # it keeps getting sent out but always times out and never succeeds
        timeout *= 2
        #recompute chunk destinations after removing failed machines
        bad_machine_indices = [i for i,val in enumerate(result) if val==None]
        # convert indices of bad machines to ips
        bad_machine_ips = set([available_servers[i] for i in bad_machine_indices])
        for server in bad_machine_ips:
            available_servers.remove(server)
        #check if list is empty. If not, reassign to remaining machines. If yes, ask for machines again
        if available_servers:
            chunk_assignments = helpers._get_chunk_assignments(available_servers, len(chunks))
        else:
            available_servers = list()
            helpers._broadcast_client_thread(MULTICAST_GROUP_IP, MULTICAST_PORT, available_servers)

    return result
    