import sys
import random
import time
import grpc
import blockchain_pb2
import blockchain_pb2_grpc


class Client:
    def __init__(self, node_address):
        self.node_address = node_address
        self.channel = grpc.insecure_channel(node_address)
        self.stub = blockchain_pb2_grpc.blockchainStub(self.channel)

    def issueTX(self, src, dst, amnt, reward):
        request = blockchain_pb2.issue_tx(src=int(src), dst=int(dst), amnt=float(amnt), reward=float(reward), timestamp=int(time.time()))
        response = self.stub.issueTX(request)
        print("TX Hash:", response.hash)

    def propBlock(self, chain_length, transactions, block_hash, prev_hash, nonce):
        request = blockchain_pb2.append_block_request(chain_length=chain_length, transactions=transactions, block_hash=block_hash, prev_hash=prev_hash, nonce=nonce)
        response = self.stub.propBlock(request)
        print(response)

    def checkHashes(self, block_index):
        request = blockchain_pb2.request_hash(block_index=block_index)
        response = self.stub.checkHashes(request)
        print(response)

    def updateReplica(self, last_common_block_index, last_common_block_hash, transactions, block_hash, prev_hash, nonce):
        request = blockchain_pb2.update_replica(last_common_block_index=last_common_block_index, last_common_block_hash=last_common_block_hash, transactions=transactions, block_hash=block_hash, prev_hash=prev_hash, nonce=nonce)
        response = self.stub.updateReplica(request)
        print(response)

    def suspend(self, temp):
        # request = blockchain_pb2.suspend_request(temp=temp)
        # response = self.stub.suspend(request)
        print('suspended')


if __name__ == "__main__":

    # Read node addresses from config file
    nodes = []
    with open("config.conf", "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 2:
                node_address = f"{parts[1]}:{parts[2]}"
                nodes.append(node_address)
    print(nodes)
    # Set up client connections to each node in the network
    clients = [Client(node) for node in nodes]

    num_txs = int(sys.argv[1])
    # Send a series of random suspend and issueTX requests to each node in the network
    for i in range(num_txs):
        client = random.choice(clients)
        #if random.random() < 0.5:
        #    print(f"Sending suspend request to {client.node_address}")
        #    client.suspend(temp=random.randint(0, 100))
        #else:
        choices = list(i for i in range(5))
        src = random.choice( choices )
        dst = random.choice( [c for c in choices if c != src] )

        amnt, reward = random.uniform(0, 10), random.uniform(0, 1)
        print(f"Sending issueTX request to {client.node_address}: src={src}, dst={dst}, amnt={amnt}, reward={reward}")
        client.issueTX(src, dst, amnt, reward)
