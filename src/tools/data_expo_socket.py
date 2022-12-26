import os
import time
import pwn
import pandas as pd


def read_planes(path):
    """
    :param path: path to the file where read the planes
    :return: a dictionary with key the `tailnum` and value the `year` of the plane
    """
    planes_data = {}
    with open(path, 'r') as f:
        f.readline()  # skip header
        line = f.readline()
        while line != "":
            line = line.strip().split(',')
            if len(line) > 8:
                # 0-
                planes_data[line[0]] = line[8]
            line = f.readline()
    return planes_data


if __name__ == "__main__":
    planes = read_planes("../datasets/planes.csv")
    files = ["../datasets/2005.csv", "../datasets/2006.csv", "../datasets/2007.csv"]
    # Parameters
    batch_size = 2000000
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # Init server
    server = pwn.listen(8888)
    # Wait for connection
    client = server.wait_for_connection()
    count_response = 0
    x = ""
    for file in files:
        with open(os.path.join(current_dir, file), "r") as f:
            header = f.readline()
            rows = f.readline()
            while rows != "" and x.strip() != "n":
                for i in range(batch_size):
                    rows += f.readline()
                client.send(rows.encode())
                # x = input("Data ready, send? (y/n) ")
                print(f"Send {count_response} of {file}")
                time.sleep(3)
                rows = f.readline()
                count_response += 1
    client.close()
    server.close()
