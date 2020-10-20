#!/usr/bin/env python
#coding:utf-8

from argparse import ArgumentParser
from datetime import datetime
import subprocess
import sys

from extractiontools.ausschnitt import BBox, logger
from extractiontools.connection import Connection, DBApp, Login


class ScriptError(BaseException):
    """
    Script could not be finished
    """

# neue DB anlegen
# bbox anlegen
# metadaten-Tabelle kopieren
# die auszuführenden auf todo setzen
# gehe die Metadaten-Tabelle durch,
# checke dependencies und setze diese ggf. auch auf aktiv
# führe alle scripte der Reihe nach aus
# zippe und kopiere ggf, FGDB zurück
# schicke email mit doenload-link


class ScriptRunner(DBApp):
    """
    Master Class to run the different scripts
    """
    def __init__(self, options):
        self.options = options
        self.set_pg_path()

    def get_os(self):
        """
        determine if run from linux or windows
        """
    def set_pg_path(self):
        """"""
        if sys.platform.startswith('win'):
            self.SHELL = False
        else:
            self.SHELL = True

    def run(self):
        """
        run the scripts
        """
        self.set_login()
        if self.options.recreate_db:
            self.update_metatable()
            self.create_db()
        self.update_script_ids()
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
                              source_db =options.source_db)
        extract.set_login(host=options.host, port=options.port, user=options.user)
        extract.get_target_boundary(bbox)
        extract.recreate_db()
        extract.extract()

    def set_login(self, password=None):
        op = self.options
        self.login = Login(host=op.host,
                           port=op.port,
                           user=op.user,
                           password=password,
                           db=op.destination_db)

    def update_script_ids(self):
        """
        Update the script ids from the master scripts
        """
        with Connection(login=self.login) as conn:
            self.conn = conn
            sql = '''
UPDATE meta.scripts s
SET id = -m.id
FROM meta.master_scripts m
WHERE s.scriptcode = m.scriptcode
AND s.id != m.id;
UPDATE meta.scripts s
SET id = m.id
FROM meta.master_scripts m
WHERE s.scriptcode = m.scriptcode
AND s.id != m.id;
            '''
            self.run_query(sql)
            self.conn.commit()


    def choose_scripts(self):
        """
        mark selected scripts in the target database
        """
        with Connection(login=self.login) as conn:
            self.conn = conn
            sql = '''
UPDATE meta.scripts SET todo = False;
            '''
            self.run_query(sql)

            sql = '''
UPDATE meta.scripts SET todo = True WHERE scriptcode = %(sc)s
            '''
            cursor = self.conn.cursor()
            if self.options.scripts is not None:
                for script in self.options.scripts:
                    cursor.execute(sql, {'sc': script})
            self.conn.commit()

    def run_scripts(self):
        """
        go through the scripts table and run the scripts to be done
        """
        sql = """
SELECT id, scriptcode, scriptname, parameter
FROM meta.script_view
WHERE todo
ORDER BY id
        """
        started_sql = """
UPDATE meta.scripts
SET started = True, success=NULL, starttime = %(time)s, endtime=NULL
WHERE scriptcode = %(sc)s;
        """

        finished_sql = """
UPDATE meta.scripts
SET success = True, started=False, endtime = %(time)s, todo = False
WHERE scriptcode = %(sc)s;
            """

        error_sql = """
UPDATE meta.scripts
SET success = False, endtime = %(time)s
WHERE scriptcode = %(sc)s;
"""

        msg_start = '''
run script {name} with parameters {params} at {time}:
{command}'''

        msg_end = '''
script {name} finished at {time} with returncode {ret}'''

        cmd = '. ~/.profile; {scriptname} {params}'

        with Connection(login=self.login) as conn:
            self.conn = conn
            cursor = self.conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                starttime = datetime.now()
                if row.parameter:
                    params = row.parameter.format(**self.options.__dict__)
                else:
                    params = ''
                command = cmd.format(scriptname=row.scriptname,
                                     params=params)
                logger.info(msg_start.format(name=row.scriptname,
                                             params=row.parameter,
                                             time=starttime,
                                             command=command))

                self.run_query(started_sql, values={'sc': row.scriptcode,
                                                    'time': starttime})
                self.conn.commit()
                pipe = subprocess.Popen(command, shell=self.SHELL)
                ret = pipe.wait()

                endtime = datetime.now()

                if ret:
                    self.run_query(error_sql, values={'sc': row.scriptcode,
                                                      'time': endtime})
                    msg = '{script} returned ErrorCode {code}'
                    self.conn.commit()
                    raise ScriptError(msg.format(script=command, code=ret))
                self.run_query(finished_sql, values={'sc': row.scriptcode,
                                                     'time': endtime})

                self.conn.commit()

                logger.info(msg_end.format(name=row.scriptname,
                                           time=endtime,
                                           ret=ret))

    def update_metatable(self):
        """update the row in the meta-table of the database"""
        sql_delete = """
DELETE FROM meta_master.projekte WHERE projektname_kurz = '{name}';
        """
        sql_insert = """
INSERT INTO meta_master.projekte (
  projektname_kurz,
  projektname_lang,
  projektnummer,
  bearbeiter,
  srid,
  "left",
  "right",
  "top",
  "bottom",
  date_areas,
  date_timetable)
VALUES (%(destination_db)s,
        %(name_long)s,
        %(project_number)s,
        %(bearbeiter)s,
        %(srid)s,
        %(left)s,
        %(right)s,
        %(top)s,
        %(bottom)s,
        %(date_areas)s,
        %(date_timetable)s
        );
        """
        op = self.options
        login = Login(host=op.host,
                      port=op.port,
                      user=op.user,
                      db=op.source_db)

        with Connection(login) as conn:
            self.conn = conn
            cursor = self.conn.cursor()
            cursor.execute(sql_delete.format(name=op.destination_db))
            cursor.execute(sql_insert, op.__dict__)
            self.conn.commit()


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--name-long', action="store",
                        help="Long Project Name",
                        dest="name_long", default='Projektname')


    parser.add_argument('--projectnumber', action="store",
                        help="Project Number",
                        dest="project_number", default='0000')

    parser.add_argument('--bearbeiter', action="store",
                        help="Bearbeiter",
                        dest="bearbeiter", default='ggr')



    parser.add_argument("-s", '--srid', action="store",
                        help="srid of the target database", type=int,
                        dest="srid", default='31467')

    parser.add_argument("-t", '--top', action="store",
                        help="top", type=float,
                        dest="top", default=55.6)
    parser.add_argument("-b", '--bottom,', action="store",
                        help="bottom", type=float,
                        dest="bottom", default=55.61)
    parser.add_argument("-r", '--right', action="store",
                        help="right", type=float,
                        dest="right", default=10.0)
    parser.add_argument("-l", '--left', action="store",
                        help="left", type=float,
                        dest="left", default=9.9)

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='localhost')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    parser.add_argument('--date-areas', action="store",
                        help="Stichtag Gebietsstand",
                        dest="date_areas", default='01.01.2016')

    parser.add_argument('--date-timetable', action="store",
                        help="Stichtag Fahrplandaten",
                        dest="date_timetable", default='01.01.2016')

    parser.add_argument('--folder', action="store",
                        help="Ordner, in den die FGDB kopiert werden soll",
                        dest="folder")

    parser.add_argument('--scripts', action="store",
                        help="Scripte, die ausgeführt werden sollen",
                        nargs='+',
                        dest="scripts")

    parser.add_argument('--recreate', action="store_true",
                       help="Erzeuge Zieldatenbank neu",
                       dest="recreate_db")



    options = parser.parse_args()

    script_runner = ScriptRunner(options)
    script_runner.run()