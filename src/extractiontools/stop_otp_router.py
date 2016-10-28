import os
import sys
from shutil import move
from argparse import ArgumentParser
from subprocess import Popen, PIPE, call
from extractiontools.otp_config import OTP_JAR


def kill_process_on_port(port):
    cmd = """netstat -nlp|grep :{port} """.format(port=port) + \
        """| awk '{ print $7 }'  | sed 's/\/java//'"""
    p1 = Popen([cmd, ], shell=True, stdout=PIPE)
    pid = p1.stdout.read().strip()
    if pid:
        if pid == '-':
            raise IOError("""
    There is a process running on port {port}, but it was started by another user.
    It cannot be killed. Please try out 'sudo netstat -nlp|grep :{port}'
    and kill the process manually with kill `pid'""".format(port=port))
        p5 = Popen(['kill', '{}'.format(pid)])


def main():
    parser = ArgumentParser(description="OTP Routererzeugung")


    parser.add_argument("--port", '-p', action="store", type=int,
                        help="port on which the OTP HTTP Server should listen",
                        dest="port", default=7789)

    parser.add_argument("--secure-port", '-s', action="store", type=int,
                        help="port on which the OTP HTTPS Server should listen",
                        dest="secure_port", default=7788)

    args = parser.parse_args()

    for port in (args.port, args.secure_port):
        kill_process_on_port(port)

if __name__ == "__main__":
    main()