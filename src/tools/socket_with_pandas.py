import argparse
import os

import pandas as pd
import pwn


def setup_parser():
    parser = argparse.ArgumentParser(
        prog='socker_with_pandas.py',
        description='Read csv files and send them to a socket',
        epilog='The default port is 8888 with a chunk size of 2000000 lines')
    parser.add_argument('-p', '--port', type=int, default=8888,
                        help='The port to listen on')
    parser.add_argument('-c', '--chunk', type=int, default=2000000,
                        help='The number of lines to send at once')
    parser.add_argument('-f', '--files', nargs='+',
                        default=["../datasets/2005.csv", "../datasets/2006.csv", "../datasets/2007.csv"],
                        help='The files to read')

    return parser.parse_args()


if __name__ == "__main__":
    # Set up the parser input
    args = setup_parser()
    # Init variables
    listen_port = args.port
    chunk_size = args.chunk
    files = args.files
    print(files)
    # Open files
    current_dir = os.path.dirname(os.path.realpath(__file__))
    df_airplanes = pd.read_csv("../datasets/plane-data.csv")
    df_airports = pd.read_csv("../datasets/airports.csv")
    # Init the server and wait for client connection
    server = pwn.listen(listen_port)
    client = server.wait_for_connection()
    # Main loop read the file, join with airplanes and send to the client
    for file in files:
        df = pd.read_csv(os.path.join(current_dir, file), chunksize=chunk_size)
        for chunk in df:
            # Merge with airplanes
            chunk = chunk.merge(df_airplanes[["tailnum", "year"]], left_on="TailNum", right_on="tailnum", how="left")
            # Merge with airports
            df_airports = df_airports[["iata", "state"]]
            chunk = chunk.merge(df_airports, left_on="Origin", right_on="iata", how="left")
            chunk = chunk.merge(df_airports, left_on="Dest", right_on="iata", how="left")
            # Drop redundant columns and fill NaN
            chunk = chunk.fillna(0)
            client.send(chunk.to_csv(
                index=False,
                header=False,
                columns=[
                    "Year", "Month", "DayofMonth", "DayOfWeek",
                    "DepTime", "TailNum", "ActualElapsedTime",
                    "CRSElapsedTime", "Origin", "Dest",
                    "year", "state_x", "state_y"
                ]
            ).encode())
    # Close connections
    client.close()
    server.close()
