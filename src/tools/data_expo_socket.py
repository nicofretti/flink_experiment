import os
import time

import pwn

if __name__ == "__main__":
    files = ["../datasets/2005.csv", "../datasets/2006.csv"]
    batch_size = 500
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # Init server
    server = pwn.listen(8888)
    # Wait for connection
    client = server.wait_for_connection()
    # Close connections
    with open(os.path.join(current_dir, files[0]), "r") as f:
        header = f.readline()
        rows = f.readline()
        while rows != "":
            for i in range(batch_size):
                rows += f.readline()
            client.send(rows.encode())
            rows = f.readline()
    client.close()
    server.close()
