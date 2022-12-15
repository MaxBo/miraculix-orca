import time
import os
import logging
from shutil import move
from argparse import ArgumentParser
from subprocess import Popen, call, PIPE
from typing import Dict

from extractiontools.otp_config import OTP_JAR, JAVA


class OTPServer:
    def __init__(self,
                 ports: Dict[str, int],
                 base_path: str='',
                 graph_subfolder: str='otp_graphs',
                 routers: Dict[str, str]=None,
                 start_analyst: bool=True
                 ):
        """"""
        self.base_path = base_path.replace('~', os.environ['HOME'])
        self.graph_subfolder = graph_subfolder
        self.routers = routers
        self.ports = ports
        self.start_analyst = start_analyst
        self.logger = logger or logging.getLogger(self.__module__)


    @property
    def graph_folder(self) -> str:
        return os.path.join(os.path.dirname(self.base_path),
                            self.graph_subfolder)

    def get_otp_build_folder(self, project: str, subfolder: str) -> str:
        """get the build folder"""
        build_folder = os.path.join(self.base_path,
                                    project,
                                    subfolder,
                                    )
        return build_folder

    def start(self):
        """"""
        graph_folder = self.graph_folder

        process_args = [JAVA, '-Xmx2G', '-jar', OTP_JAR, '--graphs',
                        graph_folder, ]

        for router_name in self.routers:
            process_args.extend(['--router ', router_name])

        # stop router if exists
        self.stop()

        analyst = '--analyst' if self.start_analyst else None
        port = self.ports['port']
        secure_port = self.ports['secure_port']

        process_args.extend(['--server ',
                             '--port ', f'{port}',
                             '--securePort ', f'{secure_port}',
                             analyst])

        # start Router
        print(process_args)
        p2 = Popen(process_args)

        self.logger.info("Server started")

    def stop(self):
        for port in self.ports.values():
            self.kill_process_on_port(port)

    @staticmethod
    def kill_process_on_port(port):
        cmd = (f"netstat -nlp|grep :{port}"
               "| awk '{ print $7 }'  | sed 's/\/java//'")
        p1 = Popen([cmd, ], shell=True, stdout=PIPE)
        pid = p1.stdout.read().decode('utf-8').strip()
        if pid:
            if pid == '-':
                raise IOError(f"""
        There is a process running on port {port}, but it was started by another user.
        It cannot be killed. Please try out 'sudo netstat -nlp|grep :{port}'
        and kill the process manually with kill `pid'""")
            p5 = Popen(['kill', f'{pid}'])

    def create_router(self,
                      build_folder: str,
                      target_folder: str,
                     ):
        """create OTP router"""
        callargs = [JAVA, '-Xmx12G', '-jar', OTP_JAR, '--build', build_folder]
        cmd = ' '.join(callargs)
        self.logger.info(cmd)
        call(callargs)

        graph_file = os.path.join(build_folder, "Graph.obj")
        if not os.path.exists(target_folder):
            os.makedirs(target_folder)
        dst_file = os.path.join(target_folder, "Graph.obj")
        if os.path.exists(dst_file):
            os.remove(dst_file)
            print("overwriting old file...")
        move(graph_file, dst_file)
        self.logger.info(f"Graph moved to {dst_file}")


def main():
    parser = ArgumentParser(description="OTP Routererzeugung")

    parser.add_argument('--base-path', action="store", type=str,
                        help="folder to store the resulting gtfs files",
                        dest="base_path", default=r'~/gis')

    parser.add_argument("--destination-db", "-n", action="store", nargs='+',
                        help="names of the routers",
                        dest="names", required=True)

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



    routers = dict()
    n_names = len(args.names)
    n_suffix = len(args.suffix)
    n_routers = max(n_names, n_suffix)
    if (n_names > 1 and n_suffix > 1 and n_names != n_suffix):
        raise ValueError(
            'wrong number of suffixes ({}) given for {} names'.format(n_names,
                                                                      n_suffix))
    for i in range(n_routers):
        name = args.names[0] if n_names == 1 else args.names[i]
        suffix = args.suffix[0] if n_suffix == 1 else args.suffix[i]
        router_name = '_'.join((name, suffix))
        routers[router_name] = name

    otp_server = OTPServer(ports=dict(port=args.port,
                                      secure_port=args.secure_port),
                           base_path=args.base_path,
                           graph_folder=args.graph_folder,
                           routers=routers,
                           start_analyst=args.anayst)
    otp_server.start()


if __name__ == "__main__":
    main()
