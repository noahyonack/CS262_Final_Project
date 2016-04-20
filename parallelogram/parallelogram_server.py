import helpers
import threading
import Queue
import cloudpickle as pickle
import time

class Server(threading.Thread):
    def __init__(self, port):
        self.queue = Queue.Queue()
        self.port = port
        threading.Thread.__init__(self)

    def run(self):
        self.sstr = threading.Thread(
            target = helpers._server_socket_thread_receive, 
            args = (self.port, self.queue))
        # makes sure the thread exits when the server exits so the 
        # whole program can exit. Doesn't elegantly shut down, but
        # seems to have no effect on the resources
        self.sstr.daemon = True
        self.sstr.start()
        while True:
            if not self.queue.empty():
                dict_received = pickle.loads(self.queue.get())
                chunk = dict_received['chunk']
                func = dict_received['func']
                op = dict_received['op']

                if op == 'map':
                    processed_chunk = helpers._single_map(func, chunk)
                elif op == 'filter':
                    processed_chunk = helpers._single_filter(func, chunk)
                elif op == 'reduce':
                    processed_chunk = helpers._single_reduce(func, chunk)
                else:
                    processed_chunk = 'This operation does not exist.'
                
                dict_sent = {
                    'chunk': processed_chunk, 
                    'index': dict_received['index']
                }

                self.ssts = threading.Thread(
                    target = helpers._server_socket_thread_send, 
                    args = (self.port + 1, pickle.dumps(dict_sent))
                )
                self.ssts.start()

if __name__ == '__main__':
    a = Server(1100)
    a.start()