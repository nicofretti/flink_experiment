import os

import pandas as pd
import pwn

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.realpath(__file__))
    files = ["../datasets/2005.csv"]
    chunk_size = 100
    df_airplanes = pd.read_csv("../datasets/plane-data.csv")
    # Init the server and wait for client connection
    # server = pwn.listen(8888)
    # client = server.wait_for_connection()
    # Main loop read the file, join with airplanes and send to the client
    for file in files:
        df = pd.read_csv(os.path.join(current_dir, file), chunksize=chunk_size)
        for chunk in df:
            chunk = chunk.merge(df_airplanes[["tailnum", "year"]], left_on="TailNum", right_on="tailnum", how="left")\
                .fillna(0).drop("tailnum", axis=1)
            print(chunk.to_csv(
                index=False,
                header=False
            ).encode())
            exit(0)
    # Close connections
    # client.close()
    # server.close()