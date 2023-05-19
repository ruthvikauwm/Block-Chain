import sys, os
import pickle

import time
import signal
from random import choice, sample

import grpc
from concurrent import futures

import blockchain_pb2
import blockchain_pb2_grpc

from blockchain import Blockchain, Transaction
from datetime import datetime

NUM_TO_BATCH = 10

def get_wallets():
    with open("logs/wallets.out", 'rb') as handle:
        wallets = pickle.load(handle)
    return wallets



#RPC STATE UPDATE HANDLERS
##handle issued_tx msg
#   Add transaction to pending pool.
def handle_issuedTX(src, dst, amnt, reward, timestamp):
    global PENDING_POOL
    tx_str = str(src) + " " + str(dst) + " " + str(amnt) + " " + str(reward) + " " + str(timestamp)
    PENDING_POOL.append(tx_str)
    print("NUM PENDING TXS:", len(PENDING_POOL))
    return hash( ((hash((src, dst)), hash((reward, amnt))), timestamp) ) 

##handle append_block_request
#   If block valid, append to chain. Else, update or do nothing.
def handle_block_append_request(chain_length, block_dict):
    global BLOCKCHAIN
    print("Receiving block_append_request")
    block_status = False
    if chain_length == len(BLOCKCHAIN.blocks) + 1 and block_dict["prev_hash"] == BLOCKCHAIN.blocks[-1].block_hash:
        try:
            BLOCKCHAIN.add_block(block_dict["LOCAL_ID"], block_dict["txs"],
                                 block_dict["prev_hash"], block_dict["nonce"])
            block_status = True
            print("Valid block appended to chain...")
        except Exception as e:
            print("Append failure; block invalid...", e)
    else:
        print("Append failure; Chains don't match...")
        
    block_hashes = ""
    if not block_status:
        for block in BLOCKCHAIN.blocks:
            block_hashes += str(block.block_hash) + "/"
    block_hashes = block_hashes[:-1]
        
    print("CURRENT BLOCKCHAIN:")
    for block in BLOCKCHAIN.blocks:
        print("**************************************************")
        print("BLOCK with hash %s and prev hash %s" % (block.block_hash, block.prev_hash if block.prev_hash != "" else "NULL") )
        print(f'The solution is nonce = {block.nonce}')
        print("**************************************************")
        print("\n")
    return block_status, len(BLOCKCHAIN.blocks), block_hashes
    

def handle_discarded_blocks(discarded_blocks, new_blocks):
    global PREVIOUSLY_BATCHED_TXS
    TO_ADD = []
    txs_in_new_blocks = []
    for block in new_blocks:
        for tx_hash in block.tx_hashes:
            txs_in_new_blocks.append(tx_hash)
            
    for block in discarded_blocks:
        for i, tx_str in enumerate(block.tx_strs):
            if block.tx_hashes[i] not in txs_in_new_blocks and hash(tx_str) in PREVIOUSLY_BATCHED_TXS:
                TO_ADD.append(tx_str)
    print("DISCARDED TRANSACTIONS TO RE-ADD...", len(TO_ADD))
    print("PENDING POOL LEN:", len(PENDING_POOL))
    for tx_str in TO_ADD:
        print("adding...", tx_str)
        PENDING_POOL.append(tx_str)
    print("PENDING POOL LEN AFTER ADDING:", len(PENDING_POOL))
    return

def handle_update_replica(request):
    global BLOCKCHAIN, SERVER_NUM
    print("updating local replica...")
    block_status = False
    if BLOCKCHAIN.blocks[request.last_common_block_index].block_hash == request.last_common_block_hash:
        
        print("removing blocks so that replica may issue new blocks... ")
        discarded_blocks = BLOCKCHAIN.blocks[request.last_common_block_index + 1:]
        BLOCKCHAIN.blocks = BLOCKCHAIN.blocks[:request.last_common_block_index + 1]
        
        block_dict =  {"LOCAL_ID"   : request.miner,
                      "block_hash"  : request.block_hash,
                      "prev_hash"   : BLOCKCHAIN.blocks[-1].block_hash,
                      "nonce"       : request.nonce,
                      "txs"         : request.transactions.split(",")}
        try:
            BLOCKCHAIN.add_block(block_dict["LOCAL_ID"], block_dict["txs"],
                                 block_dict["prev_hash"], block_dict["nonce"])
            block_status = True
            print("Valid block appended to chain...")
            
        except Exception as e:
            print("Append failure; block invalid... Restoring original local chain.", e)
            for block in discarded_blocks:
                BLOCKCHAIN.add_block(block.server_id, block.txs,
                                     block.prev_hash, block.nonce)
    if block_status == True:
        print("UPDATED CHAIN:")
        for block in BLOCKCHAIN.blocks:
            print("**************************************************")
            print("BLOCK with hash %s and prev hash %s" % (block.block_hash, block.prev_hash if block.prev_hash != "" else "NULL") )
            print(f'The solution is nonce = {block.nonce}')
            print("**************************************************")
            print("\n")
            
        new_blocks = BLOCKCHAIN.blocks[request.last_common_block_index + 1:]
        handle_discarded_blocks(discarded_blocks, new_blocks)
    
    return BLOCKCHAIN.blocks[-1].block_hash, block_status
    
##handle_suspend
#   Suspends replica to evaluate performance during partitions
def handle_suspend():
    global SUSPENDED
    SUSPENDED = True
    return

#/////////////////////////////////////////////////////////////////////////////
##STARTUP
##INITIALIZE STATE
def init_servers():
    global SERVERS, LOCAL_ID, SERVER_NUM
    SERVERS = {}
    print("Reading from [config.conf] and initializing server...")
    with open("config.conf", "r") as f:
        for line in f:
            SERVERS[int(line.split(" ")[0])] = f'{line.split(" ")[1]}:{line.split(" ")[2].rstrip()}'
            
    SERVER_NUM = int(sys.argv[1])
    LOCAL_ID = SERVERS[SERVER_NUM]
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    blockchain_pb2_grpc.add_blockchainServicer_to_server(blockchainServices(), server)
    server.add_insecure_port(LOCAL_ID)
    server.start()
    print("Server started successfully. Local ID is", LOCAL_ID)
    return server

##INITIALIZE STATE
def init_state():
    global SERVERS, PENDING_POOL, PREVIOUSLY_BATCHED_TXS, WALLETS, BLOCKCHAIN, SUSPENDED, LOCAL_ID
    global TIME_TO_COMMIT, COMMITTED_BLOCKS, LAST_COMMITTED_BLOCK_INDEX
    global WALLET_LOG
    
    WALLETS = get_wallets()
    WALLET_LOG = [WALLETS] ##captures snapshots of wallet states
    print("Initializing state... init [WALLETS] amnts are:", WALLETS)

    SUSPENDED = False
    PENDING_POOL = [] #will contain list of rpc "issue_tx" messages & tx's waiting to be committed
    PREVIOUSLY_BATCHED_TXS = []
    
    TIME_TO_COMMIT = {} #matches tx IDs to start time. end_time tuple then to latency
    COMMITTED_BLOCKS = []
    LAST_COMMITTED_BLOCK_INDEX = 0
    
    BLOCKCHAIN = Blockchain(SERVER_NUM) #init blockchain to populate later
    return

#/////////////////////////////////////////////////////////////////////////////
#EVENT_LOOP, TIMER, BATCH_TXS, PROP_BLOCK, AND UPDATE_REPLICAS
##SET TIMER
#   When timer reaches 0, batch transactions into block
def set_timer(func): 
    heartbeat_timeout = choice(range(10, 30)) ##EDIT THIS..
    signal.signal(signal.SIGALRM, func)
    signal.alarm(heartbeat_timeout)
    return

def stop_timer():
    signal.alarm(0)
    return

def update_replica(last_matching_index_, last_matching_hash_, id_):
    global BLOCKCHAIN, SERVERS
    
    for block in BLOCKCHAIN.blocks[last_matching_index_ + 1:]:
        print("sending block with hash", block.block_hash, "to server", id_)
        channel = grpc.insecure_channel(SERVERS[id_])
        stub = blockchain_pb2_grpc.blockchainStub(channel)

        tx_list = ""
        for tx in block.txs:
            tx_list += tx.str + ","
        tx_list = tx_list[:-1]
        print(tx_list)
        try:
            params = blockchain_pb2.update_replica( last_common_block_index = last_matching_index_,
                                                    last_common_block_hash = last_matching_hash_,
                                                    block_hash = block.block_hash,
                                                    transactions = tx_list,
                                                    nonce = block.nonce,
                                                    miner = block.server_id )
            response = stub.updateReplica(params)
        except grpc.RpcError as e:
            err_str = "Error issuing update_replica to server " + str(id_)
            print(err_str, e)
            break
        last_matching_index_ += 1
        last_matching_hash_ = BLOCKCHAIN.blocks[last_matching_index_].block_hash

        if not response.block_status:
            print("Error updating replica...")
            break
    return

def find_last_common_index(original_response):
    global BLOCKCHAIN
    
    print("in find last common index...", original_response.block_hashes.split("/"))
    replica_block_hashes = [ int(block_hash) for block_hash in original_response.block_hashes.split("/") ]
    local_block_hashes = [ block.block_hash for block in BLOCKCHAIN.blocks ]
    local_block_hashes = local_block_hashes[:len(replica_block_hashes)]

    for i in range(1, len(replica_block_hashes) + 1):
        print("local_block_hash at:", -i, ":", local_block_hashes[-i], "replica:", replica_block_hashes[-i])
        if local_block_hashes[-i] == replica_block_hashes[-i]:
            last_matching_index = len(replica_block_hashes) - 1 - i
            print("FOUND LAST COMMON BLOCK... it is:", BLOCKCHAIN.blocks[last_matching_index].block_hash)
            last_matching_hash = BLOCKCHAIN.blocks[last_matching_index].block_hash
            break                  
    return last_matching_index, last_matching_hash

def check_if_replica_needs_updated(original_response, id_):
    global BLOCKCHAIN, SERVERS, SERVER_NUM
    if original_response.status == False:
        if original_response.chain_len < len(BLOCKCHAIN.blocks):
            print("ATTEMPTING TO UPDATE REPLICA WITH ID:", id_)
            if original_response.chain_len <= 2:
                last_matching_index = 0
                last_matching_hash = 0
                print("FOUND LAST COMMON BLOCK... it is:", BLOCKCHAIN.blocks[last_matching_index].block_hash)
            else:
                last_matching_index, last_matching_hash = find_last_common_index(original_response)
                
            update_replica(last_matching_index, last_matching_hash, id_)  
    return

def prop_block():
    global BLOCKCHAIN, SERVERS, LOCAL_ID, SERVER_NUM, PENDING_POOL
    for id_ in SERVERS.keys():
        if SERVER_NUM == id_:
            continue
        else:            
            channel = grpc.insecure_channel(SERVERS[id_])
            stub = blockchain_pb2_grpc.blockchainStub(channel)
            
            tx_list = str([tx.str for tx in BLOCKCHAIN.blocks[-1].txs])[1:-1]
            try:
                params = blockchain_pb2.append_block_request(chain_length = len(BLOCKCHAIN.blocks),
                                                             transactions = tx_list,
                                                             block_hash = BLOCKCHAIN.blocks[-1].block_hash,
                                                             prev_hash = BLOCKCHAIN.blocks[-1].prev_hash,
                                                             nonce = BLOCKCHAIN.blocks[-1].nonce,
                                                             src_server = SERVER_NUM
                                                             )
                response = stub.propBlock(params)
            except grpc.RpcError as e:
                err_str = "Error issuing prop_block to server " + str(id_)
                print(err_str, e)

            try:
                check_if_replica_needs_updated(response, id_)
            except:
                print("Error attempting to update server ", id_)
    return

def batch_txs():
    global PENDING_POOL, PREVIOUSLY_BATCHED_TXS, TIME_TO_COMMIT
    print("Batching transactions... PENDING POOL LEN: ", len(PENDING_POOL))
    tx_list = []
    
    if len(PENDING_POOL) > NUM_TO_BATCH:
        num_txs = NUM_TO_BATCH
    else:
        num_txs = len(PENDING_POOL)
        
    tx_str_list = sample(PENDING_POOL, num_txs)
    for tx_str in tx_str_list:
        #print("Adding [%s] to block..." % tx_str)
        tx_list.append( Transaction(tx_str) )
        PREVIOUSLY_BATCHED_TXS.append( hash(tx_str) )
        TIME_TO_COMMIT[tx_str] = { "start":datetime.now() }
        
    NEW_PENDING = []
    for tx_str in PENDING_POOL:
        if tx_str not in tx_str_list:
            NEW_PENDING.append(tx_str)
    PENDING_POOL = NEW_PENDING
    print("PENDING POOL LEN IS NOW:", len(PENDING_POOL))
    return tx_list

def create_block(signum, frame):
    global BLOCKCHAIN, LOCAL_ID, SERVER_NUM
    stop_timer() ##stop the timer...
    
    if len(PENDING_POOL) == 0:
        set_timer(create_block)
        return
    print("\n")
    print("Creating block...")
    tx_list = batch_txs()
    print("Appending new block to chain...")
    timed_out = BLOCKCHAIN.add_block(SERVER_NUM, tx_list, BLOCKCHAIN.blocks[-1].block_hash)
    if not timed_out:    
        print("Propagating...")
        prop_block()
        print("\n")
    else:
        for tx in tx_list:
            print("BLOCK MINED TOO SLOW... re-adding to PENDING POOL:", tx.str)
            PENDING_POOL.append(tx.str)
    set_timer(create_block)
    return 
    

def commit_tx(tx, block_miner):
    global WALLETS
    #print("ISSUING TX", tx)
    #print("WALLETS BEFORE:", WALLETS)
    WALLETS[tx.src] -= tx.amnt
    WALLETS[tx.dst] += tx.amnt
    WALLETS[block_miner] += tx.reward
    WALLET_LOG.append( list(WALLETS) )
    return

def commit_blocks():
    global BLOCKCHAIN, COMMITTED_BLOCKS, TIME_TO_COMMIT, LAST_COMMITTED_BLOCK_INDEX, WALLETS
    if len(BLOCKCHAIN.blocks) < 4:
        return
    
    printed_note = False
    for block in BLOCKCHAIN.blocks[LAST_COMMITTED_BLOCK_INDEX + 1:-3]: ##block must be at least 3 blocks back to be committed
        if block.block_hash not in COMMITTED_BLOCKS:    
            if not printed_note:
                print("COMMITING TXS...")
                print("WALLETS BEFORE:", WALLETS)
                printed_note = True

            for tx in block.txs:
                commit_tx(tx, block.server_id)
                if tx.str in TIME_TO_COMMIT.keys():
                    TIME_TO_COMMIT[tx.str]["end"] = datetime.now()
                        
            COMMITTED_BLOCKS.append(block.block_hash)
            LAST_COMMITTED_BLOCK_INDEX += 1
    if printed_note:
        print("WALLETS AFTER:", WALLETS)
        print("COMMITTED BLOCKS:", COMMITTED_BLOCKS)

    return
                
    
##EVENT LOOP COORDINATING OUT-GOING RPC
def event_loop(server):
    global SUSPENDED, LOCAL_ID
    try:
        set_timer(create_block)
        
        while True: 
            
            if SUSPENDED: ##suspending to invoke leader re-election
                print("Suspending...")
                server.stop(0)
                time.sleep(15)
                server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
                blockchain_pb2_grpc.add_blockchainServicer_to_server(blockchainServices(), server)
                server.add_insecure_port(LOCAL_ID)
                server.start()
                SUSPENDED = False
                print("Server re-started...")
            #print("waiting...")
            #time.sleep(5)
            commit_blocks()
            ##TODO: check if 5th back block committed yet... if not, commit it
    except Exception as e:
        print(e)
    return

#/////////////////////////////////////////////////////////////////////////////
##DIRECT INCOMING RPC
class blockchainServices(blockchain_pb2_grpc.blockchainServicer):
    ##HANDLE RPC CALLS....
    ##handles incoming RPC and changes state accordingly
    def issueTX(self, request, context):
        #print("handling transaction...")
        tx_hash = handle_issuedTX(request.src, request.dst, request.amnt, request.reward, request.timestamp)
        resp = blockchain_pb2.tx_hash(hash = str(tx_hash))
        return resp
    
    def propBlock(self, request, context):
        block_dict = {"LOCAL_ID"    : request.src_server,
                      "block_hash"  : request.block_hash,
                      "prev_hash"   : request.prev_hash,
                      "nonce"       : request.nonce,
                      "txs"         : request.transactions.split(",")}
        
        block_status, local_chain_length, local_block_hashes = handle_block_append_request(request.chain_length, block_dict)
        resp = blockchain_pb2.block_status(status = block_status, chain_len = local_chain_length, block_hashes = local_block_hashes)
        return resp

    def suspend(self, request, context):
        handle_suspend()
        return

    def updateReplica(self, request, context):
        last_block_hash_, block_status_ = handle_update_replica(request)
        resp = blockchain_pb2.replica_status(last_block_hash = last_block_hash_,
                                             block_status = block_status_)
        return resp


##/////////////RECORD EVENTS & PRINT FINAL CHAIN
#
#
def save_to_pickle_file(data, file_name):
    if not os.path.exists("logs/"):
        os.makedirs("logs/")
    with open("logs/" + file_name, 'wb') as handle:
        pickle.dump(data, handle)
    return

def save_logs(commit_times):
    global WALLET_LOG, COMMITTED_BLOCKS, BLOCKCHAIN, WALLETS
    
    tx_list = []
    for block in BLOCKCHAIN.blocks:
        for tx in block.tx_strs:
            tx_list.append(tx)
    save_to_pickle_file(WALLETS, "wallets.out")
    save_to_pickle_file(tx_list, "transactions.out") ##ordered list of transactions
    save_to_pickle_file(WALLET_LOG, "wallet_log.out") ##log of wallet states
    save_to_pickle_file(COMMITTED_BLOCKS, "committed_blocks.out") ##list of committed blocks
    save_to_pickle_file(commit_times, "tx_commit_times.out") ##commit times in seconds of each tx
    return

def print_commit_times_and_wallet():    
    commit_times = {}
    uncommitted_txs = 0

    for tx_key, tx_dict in TIME_TO_COMMIT.items():
        if "end" in tx_dict.keys() and tx_key not in commit_times.keys():
            commit_times[tx_key] = ( tx_dict["end"] - tx_dict["start"] ).total_seconds()
        elif "end" not in tx_dict.keys():
            uncommitted_txs += 1
            
    print("WALLETS AT END:", WALLETS)
    print("COMMITTED BLOCKS:", COMMITTED_BLOCKS)
    print("COMMIT TIMES:", commit_times)
    print("NUM TXS IN PENDING POOL:", uncommitted_txs)
    return commit_times

def txs_in_uncommitted_blocks():
    global BLOCKCHAIN, COMMITTED_BLOCKS
    num = 0
    for block in BLOCKCHAIN.blocks[len(COMMITTED_BLOCKS) + 1:]:
        num += len(block.txs)
    print("NUM TXS IN UNCOMMITTED BLOCK:", num)
    print("\n")
    return
    

def print_block_chain():
    global BLOCKCHAIN
    for block in BLOCKCHAIN.blocks:
        print("**************************************************")
        print("BLOCK with hash %s and prev hash %s" % (block.block_hash, block.prev_hash if block.prev_hash != "" else "NULL") )
        if len(block.txs) == 0:
            print("No transactions in block...")
        else:
            print("%d transactions:" % len(block.txs))
            for tx in block.txs:
                print(tx)
        print(f'The solution is nonce = {block.nonce}')
        print("**************************************************")
        print("\n")
    return

if __name__ == "__main__":
    global TIME_TO_COMMIT, WALLETS, COMMITTED_BLOCKS
    server = init_servers()    
    init_state()

    try:
        event_loop(server)
    except KeyboardInterrupt:
        print("/n")
        print("+--------------------------------------------------------------------+")
        print("|                                                                    |")
        print("|                             OUTPUT LOG                             |")
        print("|                                                                    |")
        print("+--------------------------------------------------------------------+")

        print("FINAL BLOCKCHAIN:")
        print_block_chain()
        print("\n")
        print("FINAL WALLET AMOUNTS:")
        commit_times = print_commit_times_and_wallet()
        txs_in_uncommitted_blocks()
        save_logs(commit_times)

