import queue
import time

from jobmon.server.server_logging import jobmonLogging as logging

logger = logging.getLogger(__name__)


class MaxpssQ:
    """Singleton Queue for maxpss"""
    _q = None
    # Add an exit point
    keep_running = True

    def __init__(self, maxsize: int = 1000000):
        if MaxpssQ._q is None:
            MaxpssQ._q = queue.Queue(maxsize=maxsize)

    def get(self):
        try:
            return MaxpssQ._q.get_nowait()
        except queue.Empty:
            logger.info("Maxpss queue is empty")
            return None

    def put(self, execution_id, age=0):
        try:
            MaxpssQ._q.put_nowait((execution_id, age))
        except queue.Full:
            logger.info("Queue is full")

    def get_size(self):
        return MaxpssQ._q.qsize()

    def empty_q(self):
        # this is for unit testing
        while self.get_size() > 0:
            self.get()


# standard loopback
HOST = "127.0.0.1"
# the last of non-privileded port
PORT = 65534


def maxpss_queue_server():
    import socket
    last_heartbeat = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((HOST, PORT))
            s.listen()
            keep_running = True
            while keep_running:
                conn, addr = s.accept()
                data = conn.recv(16)
                if data:
                    id = int(str(data, 'utf8'))
                    logger.info("Get id {}".format(id))
                    "Create an exit point"
                    if id == -1:
                        # Exit server
                        logger.info("Receive signal to close the socket on port {}".format(PORT))
                        keep_running = False
                    else:
                        # put execution id to the queue
                        MaxpssQ().put(id)
                conn.close()
                # log heartbeat every 30 minutes
                current_time = time.time()
                if int(current_time - last_heartbeat) / 60 > 30:
                    logger.info("Socket {} is alive".format(PORT))
                    last_heartbeat = current_time
        except socket.error as e:
            logger.info("Socket in use. {}".format(e))
        except Exception as e:
            logger.warning("Unexpected error {}".format(e))
        finally:
            s.close()



def maxpss_queue_client(ex_id):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            # Change the msg to the same encoding as the server
            msg = str(ex_id).encode('utf8')
            # Establish connection
            s.connect((HOST, PORT))
            s.sendall(msg)
        except Exception as e:
            logger.warning("Fail to send {m} via socket due to {e}".format(m=msg, e=str(e)))
        finally:
            s.close()