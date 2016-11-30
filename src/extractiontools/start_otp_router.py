import os
from shutil import move
from argparse import ArgumentParser
from subprocess import Popen, call, PIPE
from extractiontools.otp_config import OTP_JAR
from extractiontools.stop_otp_router import kill_process_on_port
import time


def main():
    parser = ArgumentParser(description="OTP Routererzeugung")

    parser.add_argument('--base-path', action="store", type=str,
                        help="folder to store the resulting gtfs files",
                        dest="base_path", default=r'~/gis')

    parser.add_argument("--destination-db", "-n", action="store",
                        help="name of the router",
                        dest="name", required=True)

    parser.add_argument("--graph_folder", "-g", action="store",
                        help="folder with graphs",
                        dest="graph_folder")

    parser.add_argument("--suffix", action="store",
                        help="suffix for graph name",
                        dest="suffix", default='ov')


    parser.add_argument("--port", '-p', action="store", type=int,
                        help="port on which the OTP HTTP Server should listen",
                        dest="port", default=7789)

    parser.add_argument("--secure-port", '-s', action="store", type=int,
                        help="port on which the OTP HTTPS Server should listen",
                        dest="secure_port", default=7788)

    parser.add_argument("--analyst", action="store_true",
                        help="flag to start also the analyst functionality",
                        dest="analyst")

    args = parser.parse_args()

    base_path = args.base_path.replace('~', os.environ['HOME'])


    graph_folder = args.graph_folder or os.path.join(base_path,
                                                     'otp_graphs')

    router_name = '_'.join((args.name, args.suffix))

    # stop router if exists
    for port in (args.port, args.secure_port):
        kill_process_on_port(port)

    analyst = '--analyst' if args.analyst else None

    # start Router
    p2 = Popen(['java', '-Xmx2G', '-jar', OTP_JAR, '--graphs', graph_folder,
          '--router ', router_name,
          '--server ',
          '--port ', '{}'.format(args.port),
          '--securePort ', '{}'.format(args.secure_port),
          analyst])

    print "Server started"

if __name__ == "__main__":
    main()