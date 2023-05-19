#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 12 11:54:11 2023

@author: miaweaver
"""

import pickle

WALLET_AMOUNTS = [100000] * 6
print(WALLET_AMOUNTS)

with open("logs/wallets.out", 'wb') as handle:
    pickle.dump(WALLET_AMOUNTS, handle)

