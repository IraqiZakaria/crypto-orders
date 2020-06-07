import socket
import time


def is_connected():
    """
    https://stackoverflow.com/questions/20913411/test-if-an-internet-connection-is-present-in-python
    """
    try:
        # connect to the host -- tells us if the host is actually
        # reachable
        socket.create_connection(("www.google.com", 80))
        return True
    except OSError:
        pass
    return False


def check_connection():
    inner_counter = 50
    count = 0
    connection_status = False
    while not(connection_status) and count < inner_counter:
        connection_status = is_connected()
        if connection_status is False:
            time.sleep(1)
    if connection_status is False:
        raise ConnectionError