#!/usr/bin/python3

import argparse
import logging
import lower_layer
import congestion_control

def main():
    # Parse arguments
    arg_parser = argparse.ArgumentParser(description='Server', add_help=False)
    arg_parser.add_argument('-p', '--port', dest='port', action='store',
            type=int, required=True, help='Local port')
    arg_parser.add_argument('-h', '--hostname', dest='hostname', action='store',
            type=str, default='', help='Local hostname')
    arg_parser.add_argument('-q', '--queue', dest='queue_size', 
            action='store', type=int, default=0, help='Queue size')
    arg_parser.add_argument('-b', '--bandwidth', dest='bandwidth', 
            action='store', type=int, default=1, help='Bandwidth (pkts/sec)')
    arg_parser.add_argument('-d', '--delay', dest='delay', 
            action='store', type=float, default=1.0, help='Delay (secs)')
    settings = arg_parser.parse_args()

    logging.basicConfig(level=logging.INFO, 
            format='%(levelname)s: %(message)s')

    ll_endpoint = lower_layer.LowerLayerEndpoint(local_address=(settings.hostname, settings.port),
                queue_size=settings.queue_size, bandwidth=settings.bandwidth, propagation_delay=settings.delay)

    receiver = congestion_control.Receiver(ll_endpoint)
    while True:
        data = receiver.recv().decode()
        print(data, end="")

if __name__ == '__main__':
    main()
