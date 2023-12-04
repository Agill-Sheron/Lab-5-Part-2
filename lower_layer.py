import logging
import queue
import time
import socket
import threading

class LowerLayerEndpoint:
    def __init__(self, local_address=None, remote_address=None, 
            queue_size=0, bandwidth=1, propagation_delay=0.5):
        self._local_address = local_address
        self._remote_address = remote_address
        self._queue = queue.Queue(maxsize=queue_size)
        self._transmit_delay = 1/bandwidth
        self._propagation_delay = propagation_delay

        self._socket = socket.socket(type=socket.SOCK_DGRAM)
        if self._local_address is not None:
            self._socket.bind(self._local_address)
        if self._remote_address is not None:
            self._socket.connect(self._remote_address)
            self._local_address = self._socket.getsockname()

        self._shutdown = False
        self._forward_thread = threading.Thread(target=self._forward)
        self._forward_thread.daemon = True
        self._forward_thread.start()

    @property
    def transmit_delay(self):
        return self._transmit_delay

    @property
    def propagation_delay(self):
        return self._propagation_delay

    def send(self, raw_bytes):
        threading.Timer(self._propagation_delay, self._enqueue, [raw_bytes]).start()

    def _enqueue(self, raw_bytes):
        try:
            self._queue.put(raw_bytes, block=False)
        except queue.Full:
            logging.info('Lower layer queue full => dropped: {}'.format(raw_bytes)) 

    def _forward(self):
        while (not self._shutdown):
            try:
                raw_bytes = self._queue.get(block=False)
            except queue.Empty:
                raw_bytes = None

            if (raw_bytes is not None):
                result = self._socket.send(raw_bytes)
                logging.debug('Lower layer forwarded: {}'.format(raw_bytes))

            time.sleep(self._transmit_delay)

    def recv(self, max_size=4096):
        if self._remote_address is None:
            try:
                (raw_bytes, address) = self._socket.recvfrom(max_size)
            except OSError:
                return None
            self._remote_address = address
            self._socket.connect(self._remote_address)
        else:
            try:
                raw_bytes = self._socket.recv(max_size)
            except OSError:
                return None

        if len(raw_bytes) == 0:
            return None
        
        logging.debug('Lower layer received: {}'.format(raw_bytes))
        return raw_bytes

    def shutdown(self):
        if (not self._shutdown):
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
            self._shutdown = True
