import os
import time

import pwn

if __name__ == "__main__":
    files = ["../datasets/2005.csv", "../datasets/2006.csv"]
    batch_size = 1000
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # Init server
    server = pwn.listen(8888)
    # Wait for connection
    client = server.wait_for_connection()
    # Close connections
    count = 0
    with open(os.path.join(current_dir, files[0]), "r") as f:
        header = f.readline()
        rows = f.readline()
        while rows != "" and count != 2:
            for i in range(batch_size):
                rows += f.readline()
            client.send(rows.encode())
            rows = f.readline()
            time.sleep(5)
            count+=1
            print("SEND")
    time.sleep(10)
    client.close()
    server.close()
