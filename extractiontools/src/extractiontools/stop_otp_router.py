import os
import sys
from typing import Dict
from shutil import move
from argparse import ArgumentParser
from subprocess import Popen, PIPE, call
from extractiontools.otp_config import OTP_JAR
from extractiontools.start_otp_router import OTPServer


def main():
    parser = ArgumentParser(description="OTP Routererzeugung")


    parser.add_argument("--port", '-p', action="store", type=int,
                        help="port on which the OTP HTTP Server should listen",
                        dest="port", default=7789)

    parser.add_argument("--secure-port", '-s', action="store", type=int,
                        help="port on which the OTP HTTPS Server should listen",
                        dest="secure_port", default=7788)

    args = parser.parse_args()

    otp_server = OTPServer(ports=dict(port=args.port,
                                      secure_port=args.secure_port))
    otp_server.stop()


if __name__ == "__main__":
    main()