'''
This file contains config values (like the port numbers) used by our library
'''
#:port to use for socket communications
PORT = 1001
#:ip address of group  to use for multicast communications
MULTICAST_GROUP_IP = '224.3.29.71'
#:port to use for multicast communications
MULTICAST_PORT = 10000
#:the default timeout for all sockets
DEFAULT_TIMEOUT = 5
#:the max size of a queue for socket requests
MAX_CONNECT_REQUESTS = 5
#:the max buffer size for socket data
NETWORK_CHUNK_SIZE = 8192 #max buffer size to read