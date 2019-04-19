import os
from shutil import move
from argparse import ArgumentParser
from subprocess import Popen, call, PIPE
from extractiontools.otp_config import OTP_JAR, JAVA
from extractiontools.stop_otp_router import kill_process_on_port
import time


def main():
    parser = ArgumentParser(description="OTP Routererzeugung")

    parser.add_argument('--base-path', action="store", type=str,
                        help="folder to store the resulting gtfs files",
                        dest="base_path", default=r'~/gis')

    parser.add_argument("--destination-db", "-n", action="store", nargs='+',
                        help="name of the router",
                        dest="name", required=True)

    parser.add_argument("--graph_folder", "-g", action="store",
                        help="folder with graphs",
                        dest="graph_folder")

    parser.add_argument("--suffix", action="store", nargs='+',
                        help="suffix for graph name",
                        dest="suffix", default=['ov'])


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

    process_args = [JAVA, '-Xmx2G', '-jar', OTP_JAR, '--graphs',
                    graph_folder, ]

    n_names = len(args.name)
    n_suffix = len(args.suffix)
    n_routers = max(n_names, n_suffix)
    if (n_names > 1 and n_suffix > 1 and n_names != n_suffix):
        raise ValueError(
            'wrong number of suffixes ({}) given for {} names'.format(n_names,
                                                                      n_suffix))
    for i in range(n_routers):
        name = args.name[0] if n_names == 1 else args.name[i]
        suffix = args.suffix[0] if n_suffix == 1 else args.suffix[i]
        router_name = '_'.join((name, suffix))
        process_args.extend(['--router ', router_name])

    # stop router if exists
    for port in (args.port, args.secure_port):
        kill_process_on_port(port)

    analyst = '--analyst' if args.analyst else None

    process_args.extend(['--server ',
          '--port ', '{}'.format(args.port),
          '--securePort ', '{}'.format(args.secure_port),
          analyst])

    # start Router
    print(process_args)
    p2 = Popen(process_args)

    print("Server started")

if __name__ == "__main__":
    main()