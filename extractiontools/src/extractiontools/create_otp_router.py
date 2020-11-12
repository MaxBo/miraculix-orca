import os
from shutil import move
from argparse import ArgumentParser
from subprocess import call
from extractiontools.otp_config import OTP_JAR, JAVA


def main():
    parser = ArgumentParser(description="OTP Routererzeugung")

    parser.add_argument("--folder", "-f", action="store",
                        help="folder with pbf and gtfs data",
                        dest="folder")

    parser.add_argument('--subfolder', action="store",
                        help="""subfolder within the project folder
                        to store the gtfs files""",
                        dest="subfolder", default='otp')

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

    args = parser.parse_args()

    base_path = args.base_path.replace('~', os.environ['HOME'])


    folder = args.folder or os.path.join(base_path,
                                         'projekte',
                                         args.name,
                                         args.subfolder)

    graph_folder = args.graph_folder or os.path.join(base_path,
                                                     'otp_graphs')

    call([JAVA, '-Xmx12G', '-jar', OTP_JAR, '--build', folder])

    graph_file = os.path.join(folder, "Graph.obj")
    router_name = '_'.join((args.name, args.suffix))
    target_folder = os.path.join(graph_folder, router_name)
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    dst_file = os.path.join(target_folder, "Graph.obj")
    if os.path.exists(dst_file):
        os.remove(dst_file)
        print("overwriting old file...")
    move(graph_file, dst_file)
    print("Graph moved to " + dst_file)

if __name__ == "__main__":
    main()