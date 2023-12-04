#!/usr/bin/python3

import argparse
import congestion_control
import logging
import lower_layer
import sys

def main():
    # Parse arguments
    arg_parser = argparse.ArgumentParser(description='Client', add_help=False)
    arg_parser.add_argument('-p', '--port', dest='port', action='store',
            type=int, required=True, help='Remote port')
    arg_parser.add_argument('-h', '--hostname', dest='hostname', action='store',
            type=str, default="127.0.0.1", help='Remote hostname')
    arg_parser.add_argument('-q', '--queue', dest='queue_size', 
            action='store', type=int, default=0, help='Queue size')
    arg_parser.add_argument('-b', '--bandwidth', dest='bandwidth', 
            action='store', type=int, default=1, help='Bandwidth (pkts/sec)')
    arg_parser.add_argument('-d', '--delay', dest='delay', 
            action='store', type=float, default=1, help='Delay (secs)')
    arg_parser.add_argument('-s', '--slowstart', dest='use_slow_start', 
            action='store_true', help='Enable slow start')
    arg_parser.add_argument('-f', '--fastretransmit', dest='use_fast_retransmit', 
            action='store_true', help='Enable fast retransmit + fast retransmit')
    settings = arg_parser.parse_args()

    logging.basicConfig(level=logging.INFO, 
            format='%(levelname)s: %(message)s')

    ll_endpoint = lower_layer.LowerLayerEndpoint(remote_address=(settings.hostname, settings.port),
                queue_size=settings.queue_size, bandwidth=settings.bandwidth, propagation_delay=settings.delay) 
    sender = congestion_control.Sender(ll_endpoint, settings.use_slow_start, settings.use_fast_retransmit)

    for i in range(1, 4001):
        line = "Line%04d\n" % (i)
        sender.send(line.encode())

if __name__ == '__main__':
    main()
