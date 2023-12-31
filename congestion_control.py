import enum
import logging
import queue
import struct
import threading
import datetime
import matplotlib.pyplot as plt

class PacketType(enum.IntEnum):
    DATA = ord('D')
    ACK = ord('A')
    SYN = ord('S')

class Packet:
    _PACK_FORMAT = '!BI'
    _HEADER_SIZE = struct.calcsize(_PACK_FORMAT)
    MAX_DATA_SIZE = 1400 # Leaves plenty of space for IP + UDP + SWP header 

    def __init__(self, type, seq_num, data=b''):
        self._type = type
        self._seq_num = seq_num
        self._data = data

    @property
    def type(self):
        return self._type

    @property
    def seq_num(self):
        return self._seq_num
    
    @property
    def data(self):
        return self._data

    def to_bytes(self):
        header = struct.pack(Packet._PACK_FORMAT, self._type.value, 
                self._seq_num)
        return header + self._data
       
    @classmethod
    def from_bytes(cls, raw):
        header = struct.unpack(Packet._PACK_FORMAT,
                raw[:Packet._HEADER_SIZE])
        type = PacketType(header[0])
        seq_num = header[1]
        data = raw[Packet._HEADER_SIZE:]
        return Packet(type, seq_num, data)

    def __str__(self):
        return "{} {}".format(self._type.name, self._seq_num)

class Sender:
    _BUF_SIZE = 5000

    def __init__(self, ll_endpoint, use_slow_start=False, use_fast_retransmit=False):
        self._ll_endpoint = ll_endpoint
        self._rtt = 2 * (ll_endpoint.transmit_delay + ll_endpoint.propagation_delay)

        # Initialize data buffer
        self._last_ack_recv = -1
        self._last_seq_sent = -1
        self._last_seq_written = 0
        self._buf = [None] * Sender._BUF_SIZE
        self._buf_slot = threading.Semaphore(Sender._BUF_SIZE)

        # Initialize congestion control
        self._use_slow_start = use_slow_start
        self._use_fast_retransmit = use_fast_retransmit
        self._cwnd = 1

        # Congestion window graph
        self._plotter = CwndPlotter()

        # Start receive thread
        self._shutdown = False
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.start()

        self._ssthresh = 50
        self._in_slow_start = True if use_slow_start else False

        self._dup_ack_count = 0 
        self._in_fast_recovery = False

        # Construct and buffer SYN packet
        packet = Packet(PacketType.SYN, 0)
        self._buf_slot.acquire()
        self._buf[0] = {"packet" : packet, "send_time" : None}
        self._timer = None
        self._transmit(0)

    def _transmit(self, seq_num):
        slot = seq_num % Sender._BUF_SIZE

        # Send packet
        packet = self._buf[slot]["packet"]
        self._ll_endpoint.send(packet.to_bytes())
        send_time = datetime.datetime.now()

        # Update last sequence number sent   
        if (self._last_seq_sent < seq_num):
            self._last_seq_sent = seq_num

        # Determine if packet is being retransmitted
        if self._buf[slot]["send_time"] is None:
            logging.info("Transmit: {}".format(packet))
            self._buf[slot]["send_time"] = send_time
        else:
            logging.info("Retransmit: {}".format(packet))
            self._buf[slot]["send_time"] = 0

        # Start retransmission timer
        if self._timer is not None:
            self._timer.cancel()
        self._timer = threading.Timer(2 * self._rtt, self._timeout)
        self._timer.start()

    def send(self, data):
        """Called by clients to send data"""
        for i in range(0, len(data), Packet.MAX_DATA_SIZE):
            self._send(data[i:i+Packet.MAX_DATA_SIZE])

    def _send(self, data):
        # Wait for a slot in the buffer
        self._buf_slot.acquire()

        # Construct and buffer packet
        self._last_seq_written += 1
        packet = Packet(PacketType.DATA, self._last_seq_written , data)
        slot = packet.seq_num % Sender._BUF_SIZE
        self._buf[slot] = {"packet" : packet, "send_time" : None};

        # Send packet if congestion window is not full
        if (self._last_seq_sent - self._last_ack_recv < int(self._cwnd)):
            self._transmit(packet.seq_num)
        
    def _timeout(self):
        # Update congestion window
        if self._use_slow_start:
            self._cwnd = 1
            self._in_slow_start = True
        else:
            self._cwnd = max(1, self._cwnd/2)
        
        logging.debug("CWND: {}".format(self._cwnd))
        self._plotter.update_cwnd(self._cwnd)

        # Assume no packets remain in flight
        for seq_num in range(self._last_ack_recv+1, self._last_seq_sent+1):
            slot = seq_num % Sender._BUF_SIZE
            self._buf[slot]["send_time"] = 0 
        self._last_seq_sent = self._last_ack_recv

        # Sent next unACK'd packet
        self._transmit(self._last_ack_recv + 1)

    def _recv(self):
        while not self._shutdown or self._last_ack_recv < self._last_seq_sent:
            raw = self._ll_endpoint.recv()
            if raw is None:
                continue

            packet = Packet.from_bytes(raw)
            recv_time = datetime.datetime.now()
            logging.info(f"Received: {packet}")

            if packet.seq_num > self._last_ack_recv:
                self._handle_new_ack(packet.seq_num, recv_time)
            else:
                self._handle_duplicate_ack(packet.seq_num)

        self._ll_endpoint.shutdown()

    def _handle_new_ack(self, ack_num, recv_time):
        # New ACK received
        self._dup_ack_count = 0  # Reset duplicate ACK counter

        while self._last_ack_recv < ack_num:
            self._last_ack_recv += 1
            slot = self._last_ack_recv % Sender._BUF_SIZE

            send_time = self._buf[slot]["send_time"]
            if send_time is not None and send_time != 0:
                elapsed = recv_time - send_time
                self._rtt = self._rtt * 0.9 + elapsed.total_seconds() * 0.1
                logging.debug(f"Updated RTT estimate: {self._rtt}")

            self._buf[slot] = None
            self._buf_slot.release()

        if self._in_fast_recovery:
            # Exiting Fast Recovery
            self._cwnd = self._ssthresh
            self._in_fast_recovery = False
            logging.info(f"Exiting Fast Recovery, deflating cwnd to ssthresh: {self._ssthresh}")

        if self._in_slow_start and self._cwnd >= self._ssthresh:
            # Transition from Slow Start to Congestion Avoidance
            self._in_slow_start = False

        if self._in_slow_start:
            # Slow Start phase
            self._cwnd *= 2
        else:
            # Congestion Avoidance phase
            self._cwnd += 1

        logging.debug(f"CWND: {self._cwnd}")
        self._plotter.update_cwnd(self._cwnd)
        self._send_packets_if_allowed()

    def _handle_duplicate_ack(self, ack_num):
        if self._use_fast_retransmit:
            if ack_num == self._last_ack_recv:
                self._dup_ack_count += 1
                if self._dup_ack_count == 3:
                    # Fast Retransmit
                    self._transmit(self._last_ack_recv + 1)

                    # Enter Fast Recovery
                    self._ssthresh = max(self._cwnd // 2, 2)
                    self._cwnd = self._ssthresh + 3
                    self._in_fast_recovery = True
                    logging.info(f"Fast Retransmit, new ssthresh: {self._ssthresh}, inflated cwnd: {self._cwnd}")

                elif self._in_fast_recovery:
                    # Additional duplicate ACK in Fast Recovery
                    self._cwnd += 1
                    logging.info(f"Additional duplicate ACK in Fast Recovery, cwnd: {self._cwnd}")
                    self._send_packets_if_allowed()

    def _send_packets_if_allowed(self):
        while self._last_seq_sent < self._last_seq_written and \
            self._last_seq_sent - self._last_ack_recv < int(self._cwnd):
            next_seq = self._last_seq_sent + 1
            slot = next_seq % Sender._BUF_SIZE
            if self._buf[slot] is not None:
                self._transmit(next_seq)
            else:
                break

class Receiver:
    _BUF_SIZE = 1000

    def __init__(self, ll_endpoint, loss_probability=0):
        self._ll_endpoint = ll_endpoint

        self._last_ack_sent = -1
        self._max_seq_recv = -1
        self._recv_window = [None] * Receiver._BUF_SIZE

        # Received data waiting for application to consume
        self._ready_data = queue.Queue()

        # Start receive thread
        self._recv_thread = threading.Thread(target=self._recv)
        self._recv_thread.daemon = True
        self._recv_thread.start()

    def recv(self):
        return self._ready_data.get()

    def _recv(self):
        while True:
            # Receive data packet
            raw = self._ll_endpoint.recv()
            packet = Packet.from_bytes(raw)
            logging.debug("Received: {}".format(packet))

            # Retransmit ACK, if necessary
            if (packet.seq_num <= self._last_ack_sent):
                ack = Packet(PacketType.ACK, self._last_ack_sent)
                self._ll_endpoint.send(ack.to_bytes())
                logging.debug("Sent: {}".format(ack))
                continue

            # Put data in buffer
            slot = packet.seq_num % Receiver._BUF_SIZE
            self._recv_window[slot] = packet.data
            if packet.seq_num > self._max_seq_recv:
                self._max_seq_recv = packet.seq_num

            # Determine what to ACK
            ack_num = self._last_ack_sent
            while (ack_num < self._max_seq_recv):
                # Check next slot
                next_slot = (ack_num + 1) % Receiver._BUF_SIZE
                data = self._recv_window[next_slot]

                # Stop when a packet is missing
                if data is None:
                    break

                # Slot is ACK'd
                ack_num += 1
                self._ready_data.put(data)
                self._recv_window[next_slot] = None

            # Send ACK
            self._last_ack_sent = ack_num
            ack = Packet(PacketType.ACK, self._last_ack_sent)
            self._ll_endpoint.send(ack.to_bytes())
            logging.debug("Sent: {}".format(ack))

class CwndPlotter:
    def __init__(self, refresh_rate=2):
        self._start_time = datetime.datetime.now()
        self._times = [0]
        self._cwnds = [1]
        self._last_update = datetime.datetime.now()
        self._refresh_rate = refresh_rate
        self._plot()
    
    def _plot(self):
        elapsed = datetime.datetime.now() - self._last_update
        if (elapsed.total_seconds() > self._refresh_rate):
            plt.plot(self._times, self._cwnds, color='red')
            plt.xlabel('Time')
            plt.ylabel('CWND')
            plt.savefig("cwnd.png")
            self._last_update = datetime.datetime.now()

    def update_cwnd(self, cwnd):
        time = datetime.datetime.now() - self._start_time
        self._times.append(time.total_seconds())
        self._cwnds.append(cwnd)
        self._plot()