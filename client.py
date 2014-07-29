import threading
import time
import os
import cPickle
from Crypto.PublicKey import RSA 
import utils
import hashlib
import random

HEARTBEAT_INTERVAL = 60
MAX_PEERS = 5
PEER_CONTACT_INTERVAL = 120
PEER_CONTACT_TIMEOUT = 360

DATA_DIR = 'client_data' + os.urandom(4).encode('hex')  # data directory
SHARD_DIR = DATA_DIR + os.sep + 'shards/'   # shards directory
META_FILe = DATA_DIR + os.sep + 'p2pmeta'
PUBLIC_KEY_FILE = DATA_DIR + os.sep + 'public_key'
PRIVATE_KEY_FILE = DATA_DIR + os.sep + 'private_key'
LOG_FILE = DATA_DIR + os.sep + 'client.log'

SERVER_PUBLIC_KEY_FILE = 'server_public_key'

LOCAL_ADDRESS = ('localhost', random.randint(8000, 9000))
SERVER_ADDRESS = ('localhost', 8080)

client_key = RSA.generate(2048)

class Peer(object):
    def __init__(self):
        print "Client running at: %s" % str(LOCAL_ADDRESS)
        self.server_key = '' # server's public key
        self.current_peers = {} # current active peers
        self.peer_times = {}
        self.p2pfiles = []  # list of p2p files
        self.shards = []    # list of shards 
        self.peer_id = None
        self.max_peer_sem = threading.Semaphore(MAX_PEERS)   # This is to control shard serving requests
        self._load_files()
        self.logger = utils.setup_logger(LOG_FILE)
        self._reg_with_server()

        self.ping_thrd = threading.Timer(1.0, self._ping_server_thread)
        self.ping_thrd.setDaemon(True)
        self.ping_thrd.start()
        
        self.peer_thrd = threading.Timer(1.0, self._peer_contact_thread)
        self.peer_thrd.setDaemon(True)
        self.peer_thrd.start()

        self.timeout_thrd = threading.Timer(1.0, self._peer_timeout_thread)
        self.timeout_thrd.setDaemon(True)
        self.timeout_thrd.start()
    
    # Create directories (if required) 
    def _load_files(self):
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)
            os.makedirs(SHARD_DIR)
    
    #handle handshake from peers
    def handshake(self, peer_id, server_ciphered_key, address):
        server_ciphered_key = cPickle.loads(server_ciphered_key)
        self._log("handshake peer id: %s" % peer_id)
        self._log(address)
        if self.server_key.verify(peer_id, server_ciphered_key):
            self._log("peer authenticated: %s" % peer_id)
            self.server_key.verify(self.peer_id, server_ciphered_key)
            self.current_peers[peer_id] = address
            return cPickle.dumps(self.server_ciphered_key)
        else:
            None

    
    # use to perform registration with server
    def _reg_with_server(self):
        self._log("Register with server")
        client_id, server_ciphered_key = cPickle.loads(utils.get_connection(SERVER_ADDRESS).register(client_key.publickey().exportKey()))
        self.peer_id = client_id
        self.server_ciphered_key = server_ciphered_key
            
        with open(SERVER_PUBLIC_KEY_FILE, 'r') as fp:
            self.server_key = RSA.importKey(fp.read())
   
    # this thread function repeatedly pings the server and exchange latest p2p files information
    def _ping_server_thread(self):
        # send signal to server HEARTBEAT_INTERVAL
        while True:
            try:
                p2pfiles = utils.get_connection(SERVER_ADDRESS).heartbeat(self.peer_id, cPickle.dumps(self.p2pfiles))
                self._log("pinging server")
                p2pfiles = cPickle.loads(p2pfiles)
                new_files = filter(lambda x: x not in self.p2pfiles, p2pfiles)
                for x in new_files:
                    t = threading.Thread(target = self._get_file_from_server, args = (x.filename,))
                    t.start()
                    t.setDaemon(True)
                    t.join(0.1)

                for p2pfile in p2pfiles:
                    local_file = self.p2pfiles[self.p2pfiles.index(p2pfile)]
                    local_file.peer_ids.update(p2pfile.peer_ids)
            except:
                pass
            time.sleep(HEARTBEAT_INTERVAL)

    # given a filename, download this from server
    def _get_file_from_server(self, filename):
        self._log("New p2pfile on server: %s" % filename)
        p2pfile = cPickle.loads(utils.get_connection(SERVER_ADDRESS).get_p2pfile(filename))
        for peer_id in p2pfile.peer_ids.keys():
            if peer_id not in self.current_peers and peer_id != self.peer_id:
                self._log("Found new peer: %s" % peer_id)
                if p2pfile.peer_ids[peer_id] == SERVER_ADDRESS:
                    self._download_shards(p2pfile)
                else:
                    self._auth_peer(peer_id, p2pfile.peer_ids[peer_id])

    # download shards 
    def _download_shards(self, p2pfile):
        try:
            for shard in p2pfile.shard_ids:
                if shard in self.shards:
                    continue
                self._log("Downloading shard: %s" % shard)
                sh = cPickle.loads(utils.get_connection(SERVER_ADDRESS).serve_shard(shard))
                sh.save(SHARD_DIR)
                self.shards.append(sh.shard_id)
                p2pfile.peer_ids[self.peer_id] = LOCAL_ADDRESS
                updated_file = cPickle.loads(utils.get_connection(SERVER_ADDRESS).update_p2pfile(cPickle.dumps(p2pfile), self.peer_id))
                if updated_file not in self.p2pfiles:
                    self.p2pfiles.append(updated_file)
        except:
            pass

    # authenticate peer
    def _auth_peer(self, peer_id, address):
        self._log("Sending auth request to peer: %s at %s" % (peer_id, str(address)))
        try:
            peer_con = utils.get_connection(address)
            peer_cipher_key = peer_con.handshake(self.peer_id, cPickle.dumps(self.server_ciphered_key), LOCAL_ADDRESS)
            if peer_cipher_key:
                peer_cipher_key = cPickle.loads(peer_cipher_key)
                if self.server_key.verify(peer_id, peer_cipher_key):
                    self._log("Authenticated peer: %s" % peer_id)
                    self.current_peers[peer_id] = address
        except Exception as err:
            self._log("Cannot authenticate peer")

    # monitor active peers
    def _peer_contact_thread(self):
        # contact each peer after PEER_CONTACT_INTERVAL
        while True:
            try:
                for peer in self.current_peers.keys():
                    self._log("Ping peer: %s" % str(self.current_peers[peer]))
                    peer_con = utils.get_connection(self.current_peers[peer])
                    peer_con.update_file_information(cPickle.dumps(self.p2pfiles), self.peer_id)
            except Exception as err:
                self._log(err)
            time.sleep(PEER_CONTACT_INTERVAL)

    # receive ping function, also update files
    def update_file_information(self, p2pfiles, peer_id):
        self._log("Received ping: %s" % peer_id)
        self.peer_times[peer_id] = int(time.time())
        p2pfiles = cPickle.loads(p2pfiles)
        for p2pfile in p2pfiles:
            t = threading.Timer(1.0, self._process_file_info, args = (p2pfile,peer_id, ))
            t.setDaemon(True)
            t.start()
            t.join(0.1)
        return True

    # given a file it downloads all its shards from respective peers
    # also verify received shard checksum
    def _process_file_info(self, p2pfile, peer_id):
        try:
            newer_shards = list(set(p2pfile.shard_ids) - set(self.shards)) 
            peer_con = utils.get_connection(self.current_peers[peer_id])
            for shard_id in newer_shards:
                shard = peer_con.serve_shard(shard_id)
                shard = cPickle.loads(shard)
                if hashlib.md5(shard.shard).hexdigest() == shard.checksum:
                    self._log("Downloaded shard: %s, checksum: %s" % (shard.shard_id, shard.checksum))
                    shard.save(SHARD_DIR)
                    self.shards.append(shard.shard_id)
                    p2pfile.peer_ids[self.peer_id] = LOCAL_ADDRESS
                    updated_file = cPickle.loads(utils.get_connection(SERVER_ADDRESS).update_p2pfile(cPickle.dumps(p2pfile), self.peer_id))
                    if p2pfile not in self.p2pfiles:
                        self.p2pfiles.append(p2pfile)
                    else:
                        self.p2pfiles[self.p2pfiles.index(p2pfile)] = p2pfile
        except:
            pass

    # serve shards to other peers
    def serve_shard(self, shard_id):
        self.max_peer_sem.acquire()
        self._log("Serving shard: %s" % shard_id)
        with open(SHARD_DIR + shard_id, 'rb') as fp:
            shard = cPickle.load(fp)
            self.max_peer_sem.release()
            return cPickle.dumps(shard)
        self.max_peer_sem.release()
        return False

    def _log(self, msg):
        print msg
        self.logger.info(msg)

    def get_files(self):
        return cPickle.dumps(self.p2pfiles)

    # cleanup inactive peers
    def _peer_timeout_thread(self):
        while True:
            try:
                for peer in self.current_peers.keys():
                    last_seen_time = int(time.time()) - self.peer_times[peer] 
                    if last_seen_time >= PEER_CONTACT_TIMEOUT:
                        self._log("Killing peer: %s , not seen within %d seconds" % (peer, last_seen_time))
                        del self.peer_times[peer]
            except:
                pass
            time.sleep(PEER_CONTACT_TIMEOUT)


def list_files():
    s = utils.get_connection(LOCAL_ADDRESS)
    files =  cPickle.loads(s.get_files())
    for f in files: print f

if __name__ == '__main__':
    peer = Peer()
    server = utils.threaded_server(LOCAL_ADDRESS)
    server.register_instance(peer)
    t = threading.Timer(1.0, server.serve_forever)
    t.setDaemon(True)
    t.start()
    while True:
        print "Press 1 to list files"
        print "Press 0 to exit"
        arg = raw_input('Your option>')
        if not arg: pass
        if arg[0] == '1':
            list_files()
        elif arg[0] == '0':
            break

