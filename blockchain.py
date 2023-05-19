#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 22:25:36 2023

@author: miaweaver
"""
#from datetime import datetime as dt

#NOTE: 
    #referenced this link for merkle tree implementation reference:
#http://www.righto.com/2014/02/bitcoin-mining-hard-way-algorithms.html
    #reference this link for POW
#https://dev.to/envoy_/learn-blockchains-by-building-one-in-python-2kb3#:~:text=A%20Proof%20of%20Work%20algorithm,idea%20behind%20Proof%20of%20Work.

##########################################################################
import hashlib
from datetime import datetime as dt


class Blockchain:
    class Block:
        def __init__(self, server_id, tx_list = [], prev_hash = "", nonce = ""):
            self.server_id = server_id
            self.prev_hash = prev_hash
            
            if len(tx_list) > 0:
                if type(tx_list[0]) == str:
                    self.tx_strs = [ tx for tx in tx_list ]
                    self.txs = [ Transaction(tx) for tx in tx_list ]
                else:
                    self.txs = tx_list
                    self.tx_strs = [tx.str for tx in tx_list]
            else:
                self.tx_strs = [tx.str for tx in tx_list]
                self.txs = tx_list
            
            self.tx_hashes = self.get_tx_hashes()
            self.merkle_root = self.get_merkle_root( self.tx_hashes )
            self.block_hash = self.get_block_hash()
            self.nonce = self.proof_of_work() if nonce == "" else nonce
            self.valid = self.test_nonce()
                    
        def get_tx_hashes(self):
            tx_hashes = []
            for tx in self.txs:
                tx_hashes.append( hash( ((hash((tx.src, tx.dst)), hash((tx.reward, tx.amnt))), tx.ts) ) )
            return tx_hashes

        def get_merkle_root(self, tx_hashes): ##recursive func
            if len(tx_hashes) == 0:
                return 0
            if len(tx_hashes) == 1: ##base case; list len == 1 aka all pairs have been hashed
                return tx_hashes[0]
            new_tx_hashes = []
            for i in range(0, len(tx_hashes)-1, 2): ##iterate through list, increment by two as pairing together hash i with hash i + 1
                new_tx_hashes.append(hash( (tx_hashes[i], tx_hashes[i+1]) ))
            if len(tx_hashes) % 2 == 1: # odd so hash with itself
                new_tx_hashes.append(hash( (tx_hashes[-1], tx_hashes[-1]) ))
            return self.get_merkle_root(new_tx_hashes) ##after hashing pairs, call self to hash new pairs
        
        def get_block_hash(self):
            #print("Getting block hash...")
            if self.prev_hash == "":
                return self.merkle_root
            
            #print("hashing merkle root, prev_hash, and src_id:", self.merkle_root, self.prev_hash, self.server_id)
            return hash( (hash( (self.merkle_root, self.prev_hash) ), self.server_id) )
    
        def proof_of_work(self):
            nonce = 0
            if not self.block_hash == 0:

                difficulty_keys = { -5 : "ffffd", -6 : "fffffd", -7 : "ffffffd"}
                difficulty = -6
                start = dt.now()
                while hashlib.sha256(f'{self.block_hash*nonce}'.encode()).hexdigest()[difficulty:] < difficulty_keys[difficulty]: #or [-5:] < ffffd for quicker
                    nonce += 1
                end = dt.now()
                
                print("**************************************************")
                print("BLOCK MINED with hash %s and prev hash %s" % (self.block_hash, self.prev_hash if self.prev_hash != "" else "NULL") )
                if len(self.txs) == 0:
                    print("No transactions in block...")
                else:
                    print("%d transactions:" % len(self.txs))
                    for tx in self.txs:
                        print(tx)
                print(f'The solution is nonce = {nonce}')
                print("Successful hash:", hashlib.sha256(f'{self.block_hash*nonce}'.encode()).hexdigest()[difficulty:])
                print("Time to compute:", end - start)
                print("**************************************************")
            else:
                print("**************************************************")
                print("GENESIS BLOCK MINED with hash %s and prev hash %s" % (self.block_hash, self.prev_hash if self.prev_hash != "" else "NULL") )
                print("**************************************************")
                
            return nonce
    
        def test_nonce(self):
            difficulty_keys = { -5 : "ffffd", -6 : "fffffd", -7 : "ffffffd"}
            difficulty = -6
            return True if hashlib.sha256(f'{self.block_hash*self.nonce}'.encode()).hexdigest()[difficulty:] >=  difficulty_keys[difficulty] else False
        
        def __str__(self):
            return "server_id: %d; prev_hash: %s; merkle_root: %s; block_hash: %s, nonce: %s" % (self.server_id, self.prev_hash, self.merkle_root, self.block_hash, self.nonce)


    def __init__(self, server_id):
        self.blocks = [ self.Block(server_id) ]
        
    def add_block(self, src_id, tx_list, prev_hash, nonce = ""):
        timed_out = False
        if str(prev_hash) == str(self.blocks[-1].block_hash):
            new_block = self.Block(src_id, tx_list, prev_hash, nonce)

            if str(prev_hash) == str(self.blocks[-1].block_hash):
                if new_block.valid:
                    self.blocks.append(new_block)
                else:
                    raise Exception("Invalid nonce...")
            else:
                print("Block size changed during mining... Discarding block.")
                timed_out = True
        else:
            raise Exception("Invalid prev hash...")
        return timed_out


class Transaction:
    def __init__(self, string): #src, dst, amnt, reward, ts, string = ""):
        self.str = string
        self.unpack_tx_string(string)
            
    def unpack_tx_string(self, string):
        tx_items = string.split()
        if len(tx_items) != 5:
            raise Exception("Invalid transaction string...", tx_items)
            
        if "'" in tx_items[0]:
            tx_items[0] = tx_items[0][1:]

        self.src, self.dst = int(tx_items[0]), int(tx_items[1])
        self.amnt, self.reward = float(tx_items[2]), float(tx_items[3])
        if tx_items[4][-1] == "'":
            self.ts = float(tx_items[4][:-1])
        else:
            self.ts = float(tx_items[4])
        return
    
    def __str__(self):
        return "src: %d; dst: %s; amnt: %s; reward: %s, timestamp: %s" % (self.src, self.dst, self.amnt, self.reward, self.ts)