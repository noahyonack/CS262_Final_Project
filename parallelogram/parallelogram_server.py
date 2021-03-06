'''
This file defines a Server class, which allows machines to listen on 
a port for jobs. This class should be instantiated by every machine
in the distributed system that is meant to process jobs.

The main of this file is configured such that executing the file
will immediately start this server with the configuration specified
in config.py.
'''

import time # allows us to avoid busy waiting
import Queue # to hold incoming chunks
import socket # to communicate
import helpers # exposes our helper methods
import threading # allows us to use multiple threads on a single server
import cloudpickle as pickle # allows for (de)serialization
from config import PORT, MULTICAST_PORT, MULTICAST_GROUP_IP # config vars

# run sockets on localhost 
# IP_ADDRESS = 'localhost'

# gets ip address of machine on network
IP_ADDRESS = socket.gethostbyname(socket.gethostname())

class Server(threading.Thread):
    '''
    Server class that should be run on each machine that wants to act as a 
    computational entity for the system. Extends threading. Thread to make the 
    server threaded and thus nonblocking
    '''
    def __init__(self, port):
        '''
        Initializes server listening on given port
        :param port: port that server should listen on
        '''
        self.chunk_queue = Queue.Queue()
        self.port = port
        self._abort = False
        threading.Thread.__init__(self)

    def run(self):
        '''
        Runs core loop of server. It continually listens on a port and waits 
        until it receives a message, which is added to the queue. The server 
        then determines the type of operation wanted, processes the chunk, 
        and sends the results back to the calling client. It continues to 
        process these commands until the queue is empty, at which point it 
        returns to waiting
        '''
        #infinite looping listening thread to identify itself to clients
        print('Server is Running')

        self.bst = helpers._Broadcast_Server_Thread(MULTICAST_GROUP_IP, 
            MULTICAST_PORT, self.chunk_queue)
        self.bst.start()

        #infinite looping listening thread for chunks
        self.sstr = helpers._Server_Socket_Thread_Receive(IP_ADDRESS, 
            self.port, self.chunk_queue)
        self.sstr.start()
        #infinitely loops until calling process calls stop()
        while not self._abort:
            
            # sleep so we don't busy wait
            time.sleep(0.01)

            if not self.chunk_queue.empty():
                full_chunk = self.chunk_queue.get()
                dict_received = pickle.loads(full_chunk[0])
                chunk = dict_received['chunk']
                func = dict_received['func']
                op = dict_received['op']

                if op == 'map':
                    processed_chunk = helpers._single_map(func, chunk)
                elif op == 'filter':
                    processed_chunk = helpers._single_filter(func, chunk)
                elif op == 'reduce':
                    processed_chunk = helpers._single_reduce(func, chunk)
                # TODO: raise an error here
                else:
                    processed_chunk = 'This operation does not exist.'
                
                dict_sent = {
                    'chunk': processed_chunk, 
                    'index': dict_received['index']
                }

                #sends results back on port+1
                self.ssts = threading.Thread(
                    target = helpers._server_socket_thread_send, 
                    args = (full_chunk[1], self.port + 1, 
                        pickle.dumps(dict_sent))
                )
                self.ssts.start()
        self.sstr.stop() #nicely close sockets at the end


    def stop(self):
        '''
        stops server and ensures proper cleanup of sockets
        '''
        print('Server Stopped')
        self._abort = True

if __name__ == '__main__':
    a = Server(PORT)
    a.start()