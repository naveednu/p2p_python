import os
import threading
import hashlib
import xmlrpclib
import cPickle
import time
import socket
from Crypto.PublicKey import RSA

MAX_PEERS = 5
HEARTBEAT_TIMEOUT = 180
SHARD_SIZE = 64*1024

DATA_DIR = 'server_data'    # data directory 
SHARD_DIR = DATA_DIR + os.sep + 'shards/'   # shards directory
META_FILe = DATA_DIR + os.sep + 'p2pmeta'   # metadata file
#######################################
# Generate Public and Private key pair
#######################################
PUBLIC_KEY_FILE = 'server_public_key'
PRIVATE_KEY_FILE = 'server_private_key'

LOG_FILE = DATA_DIR + os.sep + 'server.log'

LOCAL_ADDRESS = ('localhost', 8080)

import utils

class Server(object):
    def __init__(self):
        print "starting server"
        self.key = '' # generate public/private key
        self.peers = {}
        self.p2pfiles = []  # list of P2PFile
        self.server_id = os.urandom(8).encode('hex')
        self.max_peer_sem = threading.Semaphore(MAX_PEERS)   # This is to control shard serving requests
        self._load_files()  # load metadata and create dirs
        self.logger = utils.setup_logger(LOG_FILE)  # setup logger
        self._load_keys()   # load publickey

        self.heartbeat_thrd = threading.Timer(HEARTBEAT_TIMEOUT, self.check_clients) #Thread to monitor alive peers
        self.heartbeat_thrd.setDaemon(True)
        self.heartbeat_thrd.start()
    
    # Create directories (if required) and load metadata
    def _load_files(self):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
            os.makedirs(SHARD_DIR)
        if os.path.isfile(META_FILe):
            with open(META_FILe, 'rb') as fp:
                self.p2pfiles = cPickle.load(fp)
                for f in self.p2pfiles: 
                    f.peer_ids.clear()
                    f.add_peer(self.server_id, LOCAL_ADDRESS)

    # load private key
    def _load_keys(self):
        if os.path.isfile(PRIVATE_KEY_FILE):
            with open(PRIVATE_KEY_FILE, 'r') as fp:
                self.key = RSA.importKey(fp.read())
        
    # save metadata information
    def save_state(self):
        with open(META_FILe, 'wb') as fp:
            cPickle.dump(self.p2pfiles, fp)
        return True
   
    # This function is used to upload a file (used by console admin)
    # Each file_data chunk is stored as a separate shard
    def upload_file(self, file_name, file_data):
        file_id = hashlib.md5(file_name).hexdigest()
        self._log("uploading file: %s, fileid: %s, received bytes: %d" % (file_name, file_id, len(file_data.data)))

        shard = utils.Shard(file_data.data, file_id)
        shard.save(SHARD_DIR)
        self._log("saved shard: %s" % shard.shard_id)

        p2pfile = utils.P2PFile(file_name)
        if p2pfile in self.p2pfiles:
            p2pfile = self.p2pfiles[self.p2pfiles.index(p2pfile)]
        else:
            p2pfile.id = hashlib.md5(file_name).hexdigest()
            self.p2pfiles.append(p2pfile)
        p2pfile.size += len(file_data.data)
        p2pfile.shard_ids.append(shard.shard_id)
        p2pfile.add_peer(self.server_id, LOCAL_ADDRESS)
        return True

    # returns p2pfiles list
    def get_files(self):
        return cPickle.dumps(self.p2pfiles)
    
    # returns filename list
    def get_file_list(self):
        return [x.filename for x in self.p2pfiles]
    
    # given a filename it returns p2pfile object
    def get_p2pfile(self, filename):
        for p2pfile in self.p2pfiles:
            if p2pfile.filename == filename:
                return cPickle.dumps(p2pfile)

    # given a p2pfile pdates it in local p2pfile list 
    def update_p2pfile(self, p2pfile, peer_id):
        p2pfile = cPickle.loads(p2pfile)
        if self.server_id in p2pfile.peer_ids:
            p2pfile.remove_peer(self.server_id)
        for f in self.p2pfiles:
            if f.filename == p2pfile.filename:
                f.peer_ids = p2pfile.peer_ids
        return cPickle.dumps(p2pfile)
        
    # serve shard given a shard id
    def serve_shard(self, shard_id):
        self.max_peer_sem.acquire()
        self._log("Serving shard: %s" % shard_id)
        with open(SHARD_DIR + shard_id, 'rb') as fp:
            shard = cPickle.load(fp)
            self.max_peer_sem.release()
            return cPickle.dumps(shard)
        self.max_peer_sem.release()
        return False
   
    # handle registration from clients
    def register(self, _key):
        client_id = hashlib.md5(_key).hexdigest()
        self._log("Register request client ID: %s" % client_id)
        server_ciphered_key = self.key.sign(client_id, None)
        self.peers[client_id] = int(time.time())
        return cPickle.dumps((client_id, server_ciphered_key))
   
    # receive heartbeats from clients
    def heartbeat(self, client_id, p2pfiles):
        self._log("Heartbeat signal client ID: %s" % client_id)
        if client_id in self.peers:
            time_now = int(time.time())
            self.peers[client_id] = time_now
        p2pfiles = cPickle.loads(p2pfiles)
        for p2pfile in p2pfiles:
            server_file = self.p2pfiles[self.p2pfiles.index(p2pfile)]
            server_file.peer_ids.update(p2pfile.peer_ids)

        return self.get_files()

    # monitor keep-alive from clients
    def check_clients(self):
        while True:
            for client in self.peers.keys():
                last_seen_time = int(time.time()) - self.peers[client]
                if last_seen_time >= HEARTBEAT_TIMEOUT:
                    self._log("Killing client: %s , not seen within %d seconds" % (client, last_seen_time))
                    map(lambda p2pfile: p2pfile.remove_peer(client), self.p2pfiles)
                    del self.peers[client]
            time.sleep(HEARTBEAT_TIMEOUT)

    def _log(self, msg):
        print msg
        self.logger.info(msg)

# helper function to print file list from server
def list_files():
    s = utils.get_connection(LOCAL_ADDRESS)
    files =  cPickle.loads(s.get_files())
    for f in files: print f

# used to upload given file to server
def upload_file(name):
    try:
        s = utils.get_connection(LOCAL_ADDRESS)
        file_list = cPickle.loads(s.get_files())
        if filter(lambda x: x.filename == name, file_list):
            print "File already exists on server"
            return
        with open(name, 'rb') as handle:
            def read_chunk():
                return handle.read(SHARD_SIZE)
            for chunk in iter(read_chunk, ''):
                data = xmlrpclib.Binary(chunk)
                s.upload_file(name, data)
            s.save_state()

    except Exception as ext:
        print "File upload error"
        print ext

if __name__ == '__main__':
    server = utils.threaded_server(LOCAL_ADDRESS)
    server.register_instance(Server())
    t = threading.Timer(1.0, server.serve_forever)
    t.setDaemon(True)
    t.start()
    print "Server running at %s" % str(LOCAL_ADDRESS)
    while True:
        print "Press 1 to list files"
        print "Press 2 to upload file"
        print "Press 0 to exit"
        arg = raw_input('Your option>')
        if not arg: pass
        elif arg[0] == '2':
            print "Please enter the name of the file to upload: "
            fname = raw_input()
            upload_file(fname)
        elif arg[0] == '1':
            list_files()
        elif arg[0] == '0':
            break
        pass
    #server.serve_forever()
