#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser
from datetime import datetime
import subprocess

from extractiontools.ausschnitt import BBox, ExtractMeta, logger
from extractiontools.connection import Connection, DBApp


class ScriptError(BaseException):
    """
    Script could not be finished
    """

# neue DB anlegen
# bbox anlegen
# metadaten-Tabelle kopieren
# die auszuführenden auf selected setzen
# gehe die Metadaten-Tabelle durch,
# checke dependencies und setze diese ggf. auch auf aktiv
# führe alle scripte der Reihe nach aus
# zippe und kopiere ggf, FGDB zurück
# schicke email mit doenload-link


class ScriptRunner(DBApp):
    """
    Master Class to run the differenz scripts
    """
    def __init__(self, options):
        self.options = options

    def run(self):
        """
        run the scripts
        """
        self.create_db()
        self.choose_scripts()
        self.run_scripts()

    def create_db(self):
        """
        (re)-create the target database
        and copy the selected files
        """
        options = self.options

        bbox = BBox(top=options.top, bottom=options.bottom,
                    left=options.left, right=options.right)

        extract = ExtractMeta(destination_db=options.destination_db,
                              target_srid=options.srid,
                              recreate_db=True,
                              source_db =options.source_db)
        extract.set_login(host=options.host, port=options.port, user=options.user)
        extract.get_target_boundary(bbox)
        extract.extract()

        self.login = extract.login1

    def choose_scripts(self):
        """
        mark selected scripts in the target database
        """
        with Connection(login=self.login) as conn:
            self.conn = conn
            sql = '''
UPDATE meta.scripts SET selected = False;
            '''
            self.run_query(sql)

            sql = '''
UPDATE meta.scripts SET selected = True WHERE scriptcode = %(sc)s
            '''
            cursor = self.conn.cursor()
            for script in self.options.scripts:
                cursor.execute(sql, {'sc': script})
            self.conn.commit()

    def run_scripts(self):
        """
        go through the scripts table and run the selected scripts
        """
        sql = """
SELECT id, scriptcode, scriptname, parameter
FROM meta.scripts
WHERE selected
ORDER BY id
        """
        started_sql = """
UPDATE meta.scripts
SET started = True, finished=False, starttime = %(time)s
WHERE scriptcode = %(sc)s;
        """

        finished_sql = """
UPDATE meta.scripts
SET finished = True, endtime = %(time)s
WHERE scriptcode = %(sc)s;
            """

        msg_start = '''
run script {name} with parameters {params} at {time}:
{command}'''

        msg_end = '''
script {name} finished at {time} with returncode {ret}'''

        cmd = '{scriptname} {params}'

        with Connection(login=self.login) as conn:
            self.conn = conn
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                starttime = datetime.now()
                params = row.parameter.format(**self.options.__dict__)
                command = cmd.format(scriptname=row.scriptname,
                                     params=params)
                logger.info(msg_start.format(name=row.scriptname,
                                             params=row.parameter,
                                             time=starttime,
                                             command=command))

                self.run_query(started_sql, values={'sc': row.scriptcode,
                                                    'time': starttime})
                self.conn.commit()
                pipe = subprocess.Popen(command)
                ret = pipe.wait()

                endtime = datetime.now()

                if ret:
                    msg = '{script} returned ErrorCode {code}'
                    raise ScriptError(msg.format(script=command, code=ret))
                self.run_query(finished_sql, values={'sc': row.scriptcode,
                                                     'time': endtime})

                self.conn.commit()

                logger.info(msg_end.format(name=row.scriptname,
                                           time=endtime,
                                           ret=ret))




def test():
    befehl = 'python ausschnitt.py -n {name} --top {top} --bottom {bottom} --srid {srid}'

    class Options(object):
        pass

    options = Options
    options.name = 'mvv'
    options.bottom = 53.2
    options.top = 99.9

    options.srid = 31467
    options.run_frnetz = False
    options.run_kfznetz = True
    befehl.format(options.__dict__)
    befehl.format(**options.__dict__)
    'python ausschnitt.py -n mvv --top 99.9 --bottom 53.2 --srid 31467'


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="srid", default='31467')

    parser.add_argument("-t", '--top', action="store",
                        help="top", type=float,
                        dest="top", default=55.6)
    parser.add_argument("-b", '--bottom,', action="store",
                        help="bottom", type=float,
                        dest="bottom", default=54.6)
    parser.add_argument("-r", '--right', action="store",
                        help="right", type=float,
                        dest="right", default=10.0)
    parser.add_argument("-l", '--left', action="store",
                        help="left", type=float,
                        dest="left", default=9.9)

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='max')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='dplus')

    parser.add_argument('--date-areas', action="store",
                        help="Stichtag Gebietsstand",
                        dest="date_areas")

    parser.add_argument('--date-timetable', action="store",
                        help="Stichtag Fahrplandaten",
                        dest="date_timetable")

    parser.add_argument('--folder', action="store",
                        help="Ordner, in den die FGDB kopiert werden soll",
                        dest="folder")

    parser.add_argument('--scripts', action="store",
                        help="Scripte, die ausgeführt werden sollen",
                        nargs='+',
                        dest="scripts")




    options = parser.parse_args()

    script_runner = ScriptRunner(options)
    script_runner.run()