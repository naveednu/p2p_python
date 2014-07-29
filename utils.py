import SocketServer
import xmlrpclib
import os
import hashlib
import cPickle
import logging
from SimpleXMLRPCServer import SimpleXMLRPCServer,SimpleXMLRPCRequestHandler
    
# Threaded mix-in
class AsyncXMLRPCServer(SocketServer.ThreadingMixIn, SimpleXMLRPCServer): pass

def threaded_server(address):
    return AsyncXMLRPCServer(address, requestHandler = SimpleXMLRPCRequestHandler, logRequests=False)

def get_connection(address):
    return xmlrpclib.ServerProxy('http://' + address[0] + ':' + str(address[1]))

def setup_logger(path):
    logger = logging.getLogger(path)
    hdlr = logging.FileHandler(path)
    print "Logging to file: %s" % path
    formatter = logging.Formatter('%(asctime)s %(levelname)s [%(funcName)s]  %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
    return logger

# data class for shards
class Shard(object):
    def __init__(self, shard, file_id):
        self.shard = shard   # shard 
        self.file_id = file_id  # file id
        self.shard_id = os.urandom(16).encode('hex')
        self.checksum = hashlib.md5(shard).hexdigest()

    # save shard with filename set to shard_id
    def save(self, path):
        with open(path + self.shard_id, 'wb') as fp:
            cPickle.dump(self, fp)
    def __str__(self):
        return "ID: %s\nFile ID: %s\nChecksum:%s" % (self.shard_id, self.file_id, self.checksum)

# data class for P2PFile
class P2PFile(object):
    def __init__(self, name):
        self.filename = name
        self.filetype = os.path.splitext(name)[1][1:]
        self.size = 0
        self.id = ''
        self.shard_ids = []
        self.peer_ids = {}
    def __eq__(self, other):
        return self.filename == other.filename
    def __ne__(self, other):
        return self.filename != other.filename
    def remove_peer(self, peer_id):
        if peer_id in self.peer_ids:
            del self.peer_ids[peer_id]
    def add_peer(self, peer_id, address):
        if peer_id not in self.peer_ids:
            self.peer_ids[peer_id] = address
    def __str__(self):
        return "Name: %s\tSize: %d, Shards: %d, Peers: %s" % (self.filename, self.size, len(self.shard_ids),self.peer_ids.values())

