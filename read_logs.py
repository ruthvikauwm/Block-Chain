#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 13:14:44 2023

@author: miaweaver
"""

import pickle

def read_log(file_name):
    with open(file_name, 'rb') as handle:
        log = pickle.load(handle)
    return log


file_names = ["logs/transactions.out", "logs/committed_blocks.out", "logs/tx_commit_times.out"]
msgs = ["ORDERED TRANSACTIONS: ", "COMMITTED BLOCKS: ", "COMMIT_TIMES: "]

for i, file_name in enumerate(file_names):
    print(msgs[i])
    log = read_log(file_name)
    if len(log) == 0:
        continue
    
    if type(log) == dict:
        for tx, commit_time in log.items():
            print("tx:", tx)
            print("commit_time:", commit_time)
        print("ALL COMMIT TIMES:")
        print(list(log.values()))
        print("\n")

    elif type(log[0]) == list:
        for item in log:
            print(item)
    else:
        print(log)
    print("\n")
