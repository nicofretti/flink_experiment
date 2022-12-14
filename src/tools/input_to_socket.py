import pwn

if __name__ == "__main__":
    # Init server
    server = pwn.listen(8888)
    # Wait for connection
    client = server.wait_for_connection()
    data = ""
    # Main loop
    while data.strip() != "exit()":
        data = input("Enter data or type exit(): ")
        client.send(data.encode())
    # Close connections
    client.close()
    server.close()
