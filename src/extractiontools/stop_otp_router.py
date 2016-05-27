import os
import sys
from shutil import move
from argparse import ArgumentParser
from subprocess import Popen, PIPE, call

OTP_JAR='/opt/repos/OpenTripPlanner/target/otp-0.20.0-SNAPSHOT-shaded.jar'

def main():
    parser = ArgumentParser(description="OTP Routererzeugung")


    parser.add_argument("--port", '-p', action="store", type=int,
                        help="port on which the OTP HTTP Server should listen",
                        dest="port", default=7789)

    parser.add_argument("--secure-port", '-s', action="store", type=int,
                        help="port on which the OTP HTTPS Server should listen",
                        dest="secure_port", default=7788)

    args = parser.parse_args()

    cmd = """netstat -nlp|grep :{port} """.format(port=args.port) + \
        """| awk '{ print $7 }'  | sed 's/\/java//'"""
    p1 = Popen([cmd, ], shell=True, stdout=PIPE)
    pid = p1.stdout.read().strip()
    if pid:
        p5 = Popen(['kill', '{}'.format(pid)])

if __name__ == "__main__":
    main()