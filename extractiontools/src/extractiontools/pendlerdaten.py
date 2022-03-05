import os
import shutil
import datetime
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
import tempfile
from extractiontools.connection import Connection, DBApp, Login

from extractiontools.ausschnitt import Extract


class ExtractRegionalstatistik(Extract):
    """
    Extract the regional statistics
    """
    tables = {}
    schema = 'regionalstatistik'
    role = 'group_osm'

    def __init__(self,
                 source_db: str,
                 destination_db: str,
                 regionalstatistik_gemeinden: str,
                 regionalstatistik_years: List[int],
                 **kwargs):
        super().__init__(destination_db=destination_db,
                         source_db=source_db, **kwargs)
        self.gemeindelayer = regionalstatistik_gemeinden
        self.jahre = regionalstatistik_years

    def additional_stuff(self):
        """
        """
        self.validate_table_exists(self.gemeindelayer)
        self.extract_erwerbstaetigkeit()

    def extract_erwerbstaetigkeit(self):
        """
        Extract Erwerbstätigkeit
        """
        self.logger.info(
            f'Extracting jobs and workers to {self.schema}.svb_jahr')
        jahre = list(int(j) for j in self.jahre)
        sql = f"""
        SELECT
          s.*
        INTO {self.schema}.svb_jahr
        FROM {self.temp}.svb_jahr s,
        {self.gemeindelayer} g
        WHERE
        g.ags = s.ags
        AND s.jahr=ANY(%s)
        """
        self.run_query(sql, vars=(jahre, ))


class ExtractPendler(Extract):
    """
    Extract the commuter statistics
    """
    tables = {}
    schema = 'pendlerdaten'
    role = 'group_osm'

    def __init__(self,
                 source_db,
                 destination_db,
                 pendlerdaten_gemeinden,
                 **kwargs):
        super().__init__(destination_db=destination_db,
                         source_db=source_db, **kwargs)
        self.gemeindelayer = pendlerdaten_gemeinden

    def additional_stuff(self):
        """
        """
        self.validate_table_exists(self.gemeindelayer)
        self.extract_pendler()

    def extract_pendler(self):
        """
        Extract Pendler
        """
        self.logger.info(
            f'Extracting commuters to {self.schema}.ein_auspendler')
        sql = f"""
        SELECT
          p.*
        INTO {self.schema}.ein_auspendler
        FROM {self.temp}.ein_auspendler p,
        {self.gemeindelayer} g
        WHERE
        (g.ags = p.ags_wo AND p."Ein_Aus" = 'Auspendler Gemeinden') OR
        (g.ags = p.ags_ao AND p."Ein_Aus" = 'Einpendler Gemeinden')
        """
        self.run_query(sql)

        sql = f"""
CREATE OR REPLACE VIEW {self.schema}.ein_auspendler_zusammengefasst AS
SELECT
e."Stichtag",
e.ags_wo,
e.gen_wo,
e.ags_ao,
e.gen_ao,
max(e.insgesamt) AS insgesamt,
max(e."Männer") AS "Männer",
max(e."Frauen") AS "Frauen",
max(e."Deutsche") AS "Deutsche",
max(e."Ausländer") AS "Ausländer",
max(e."Azubis") AS "Azubis"
FROM {self.schema}.ein_auspendler e
GROUP BY e."Stichtag", e.ags_wo, e.gen_wo, e.ags_ao, e.gen_ao
UNION
SELECT
make_date(s.jahr, 6, 30) AS "Stichtag",
s.ags AS ags_wo,
g.gen AS gen_wo,
s.ags AS ags_ao,
g.gen AS gen_ao,
s.wo_ao AS insgesamt,
NULL AS "Männer",
NULL AS "Frauen",
NULL AS "Deutsche",
NULL AS "Ausländer",
NULL AS "Azubis"
FROM {ExtractRegionalstatistik.schema}.svb_jahr s,
{self.gemeindelayer} g
WHERE s.ags = g.ags
;
"""
        self.run_query(sql)

        sql = f"""
CREATE OR REPLACE VIEW {self.schema}.pendlerbeziehungen AS
SELECT DISTINCT
e.ags_wo, e.ags_ao
FROM {self.schema}.ein_auspendler_zusammengefasst e
UNION
SELECT DISTINCT
e.ags_ao, e.ags_wo
FROM {self.schema}.ein_auspendler_zusammengefasst e
WHERE NOT e.ags_wo = e.ags_ao;
"""
        self.run_query(sql)


class ImportPendlerdaten(DBApp):
    """
    Import Commuter trips from excel-files to database
    """
    schema = 'pendlerdaten'
    role = 'group_osm'

    def __init__(self,
                 db: str = 'extract',
                 subfolder: str = 'Pendlerdaten',
                 pendlerdaten_years: List[str] = ['2020'],
                 **kwargs):
        super().__init__(schema=self.schema, **kwargs)
        self.destination_db = self.db = db
        self.set_login(database=db)
        self.check_platform()
        self.subfolder = subfolder
        self.pendlerdaten_years = pendlerdaten_years

    def run(self):
        """
        """
        with Connection(login=self.login) as conn:
            # preparation
            self.conn = conn
            self.import_pendler()
            self.conn.commit()

    def import_pendler(self):
        """import Pendlerdaten"""
        path = os.path.join(self.folder, self.subfolder)
        self.logger.info(self.pendlerdaten_years)
        for year in os.listdir(path):
            if year not in self.pendlerdaten_years:
                continue
            sub_path = os.path.join(path, year)
            for folder, dirs, files in os.walk(sub_path):
                self.logger.info(f'{folder}, {dirs}, {files}')
                for file in files:
                    fn_ext = os.path.splitext(file)
                    if len(fn_ext) < 2 or fn_ext[1] not in ['.xlsb']:
                        continue
                    self.process_file(os.path.join(folder, file))

    def process_file(self, filepath: str):
        """upload a single excel-file with Pendlerdaten"""
        self.logger.info(filepath)

        data_cols = ['insgesamt', 'Männer', 'Frauen',
                     'Deutsche', 'Ausländer', 'Azubis']

        dtype = {'ags_wo': np.unicode_, 'ags_ao': np.unicode_, }

        na_values = dict()
        for col in data_cols:
            na_values[col] = ['*', 'X']

        sheet_name = 'Auspendler Gemeinden'
        index_cols = ['ags_wo', 'gen_wo', 'ags_ao', 'gen_ao']

        df_auspendler = self.read_data(index_cols, data_cols,
                                       filepath, sheet_name,
                                       dtype, na_values)

        sheet_name = 'Einpendler Gemeinden'
        index_cols = ['ags_ao', 'gen_ao', 'ags_wo', 'gen_wo']

        df_einpendler = self.read_data(index_cols, data_cols,
                                       filepath, sheet_name,
                                       dtype, na_values)

        self.logger.info('Upload Data')
        df = df_auspendler.append(df_einpendler)
        tablename = 'pendlerdaten.ein_auspendler'

        try:
            file_name = tempfile.mktemp(suffix='.csv')
            df.to_csv(file_name, encoding='UTF8')
            cur = self.conn.cursor()
            delete_sql = f'DELETE FROM {tablename} WHERE "Bundesland" = %s AND "Stichtag" = %s'
            bundesland, stichtag = df.reset_index(
            ).loc[1, ['Bundesland', 'Stichtag']]
            cur.execute(delete_sql, (bundesland, stichtag))
            self.logger.info(f'deleted {cur.rowcount} rows in {tablename}')
            with open(file_name, encoding='UTF8') as file:
                cur.copy_expert(
                    f"COPY {tablename} FROM STDIN WITH CSV HEADER ENCODING 'UTF8';", file)
            self.conn.commit()
        finally:
            os.remove(file_name)

    def read_data(self,
                  index_cols: List[str],
                  data_cols: List[str],
                  filepath: str,
                  sheet_name: str,
                  dtype: Dict[str, np.dtype],
                  na_values: Dict[str, object],
                  ) -> pd.DataFrame:
        """read data into Dataframe"""
        cols = index_cols + data_cols
        from_cols = index_cols[:2]
        self.logger.info('Read Header')
        bundesland, stichtag, first_row = self.read_header(
            filepath, sheet_name)
        self.logger.info(f'skip {first_row} rows')

        df = pd.read_excel(filepath,
                           sheet_name=sheet_name,
                           engine='pyxlsb',
                           dtype=dtype,
                           skiprows=first_row,
                           header=None,
                           usecols=range(len(cols)),
                           skipfooter=4,
                           names=cols,
                           na_values=na_values,
                           )
        # self.logger.info(str(df))

        df[from_cols] = df[from_cols].fillna(method='ffill')
        df = df.loc[~df[index_cols[2]].isna()]

        #  mark other counties with Ü
        others = df[index_cols[3]].str.startswith('Übrige ')
        df.loc[others, index_cols[2]] = df.loc[others, index_cols[2]] + 'Ü'
        #self.logger.info('Drop Duplicates')
        df.drop_duplicates(keep='first', inplace=True)

        # make ZZ unique
        is_zz = df[index_cols[2]] == 'ZZ'
        only_zz = df.loc[is_zz]
        zz_idx = only_zz.groupby([index_cols[0]]).cumcount(
            ascending=True).astype('U')
        df.loc[is_zz, index_cols[2]] = df.loc[is_zz,
                                              index_cols[2]] + '_' + zz_idx

        df['Bundesland'] = bundesland
        df['Stichtag'] = stichtag
        df['Ein_Aus'] = sheet_name
        #self.logger.info('Set Index')
        df.set_index(['Bundesland', 'Ein_Aus', 'Stichtag', 'ags_wo', 'ags_ao'],
                     inplace=True)
        #self.logger.info('Index set')
        return df

    def read_header(self,
                    filepath: str,
                    sheet_name: str) -> Tuple[str, datetime.date, int]:
        # parse header
        # self.logger.info(str(sheet_name))
        df = pd.read_excel(filepath,
                           sheet_name=sheet_name,
                           engine='pyxlsb',
                           header=None,
                           )
        # self.logger.info(str(df))
        bundesland = df.iloc[3, 0].strip()
        stichtag = df.iloc[4, 0].split(': ')[-1].strip()
        stichtag = datetime.datetime.strptime(stichtag, "%d.%m.%Y")
        first_row = df.iloc[:, 1].first_valid_index()
        return bundesland, stichtag, first_row


class CreatePendlerSpinne(DBApp):
    """
    Import Commuter trips from excel-files to database
    """
    schema = 'pendlerdaten'
    role = 'group_osm'

    def __init__(self,
                 db: str,
                 pendlerspinne_gebiete: str,
                 target_srid: int,
                 **kwargs):
        super().__init__(schema=self.schema, **kwargs)
        self.destination_db = self.db = db
        self.set_login(database=db)
        self.check_platform()
        self.pendlerspinne_gebiete = pendlerspinne_gebiete
        self.target_srid = target_srid

    def run(self):
        """
        """
        with Connection(login=self.login) as conn:
            # preparation
            self.conn = conn
            self.validate_table_exists(self.pendlerspinne_gebiete)
            self.create_spinne()
            self.conn.commit()

    def create_spinne(self):
        """create Pendlerspinne"""

        sql = f"DROP VIEW IF EXISTS {self.schema}.spinne CASCADE;"
        self.run_query(sql)

        sql = f"""
CREATE OR REPLACE VIEW {self.schema}.spinne AS
SELECT
p.ags_wo,
p.ags_ao,
st_makeline(st_pointonsurface(g1.geom), st_pointonsurface(g2.geom))::geometry(LINESTRING, {self.target_srid}) AS geom
FROM {self.pendlerspinne_gebiete} g1,
{self.pendlerspinne_gebiete} g2,
{self.schema}.pendlerbeziehungen p
WHERE g1.ags = p.ags_wo
AND g2.ags = p.ags_ao;
"""
        self.run_query(sql)

        sql = f"""
CREATE VIEW {self.schema}.view_circle (
    ags,
    geom)
AS
SELECT p.ags,
    ST_CurveToLine(
      ST_GeomFromEWKT(
        replace(ST_AsEWKT(ST_MakeLine(p.points)), 'LINESTRING', 'CIRCULARSTRING')
        )
      )::geometry(Linestring,{self.target_srid}) AS geom
FROM (
    SELECT g.ags,
            array_agg(st_translate(st_pointonsurface(g.geom), d.dx::double precision, d.dy::double precision)) AS points
    FROM {self.pendlerspinne_gebiete} g,
            ( VALUES (2000,0), (0,'-2000'::integer), (0,2000)) d(dx, dy)
    GROUP BY g.ags
    ) p;
"""
        self.run_query(sql)

        sql = f"""
DROP MATERIALIZED VIEW IF EXISTS {self.schema}.spinne_geom;
CREATE MATERIALIZED VIEW {self.schema}.spinne_geom AS
SELECT s.*
FROM {self.schema}.spinne s
UNION
SELECT
c.ags AS ags_wo,
c.ags AS ags_ao,
c.geom
FROM {self.schema}.view_circle AS c;
        """
        self.run_query(sql)

        sql = f"""
CREATE OR REPLACE VIEW {self.schema}.pendler_spinne AS
SELECT
row_number() OVER()::integer AS rn,
e."Stichtag", s.ags_wo, s.ags_ao, s.geom,
e.insgesamt, e."Männer", e."Frauen", e."Deutsche", e."Ausländer", e."Azubis"
FROM {self.schema}.spinne_geom s,
{self.schema}.ein_auspendler_zusammengefasst e
WHERE s.ags_wo = e.ags_wo
AND s.ags_ao = e.ags_ao;
        """
        self.run_query(sql)


class ExportPendlerdaten(DBApp):
    """
    Export Pendlerdaten to Excel and Access
    """
    schema = 'pendlerdaten'
    role = 'group_osm'

    def __init__(self,
                 db: str,
                 **kwargs):
        super().__init__(schema=self.schema, **kwargs)
        self.destination_db = self.db = db
        self.set_login(database=db)
        self.check_platform()

    def export(self):
        with Connection(login=self.login) as conn:
            self.conn = conn

            folder = os.path.abspath(
                os.path.join(self.folder,
                             'projekte',
                             self.login.db,
                             'Pendlerdaten',
                             )
            )
            os.makedirs(folder, exist_ok=True)

            file_path = os.path.join(folder, 'Pendlerdaten.xlsx')
            if os.path.exists(file_path):
                os.remove(file_path)

            with pd.ExcelWriter(file_path) as excel_writer:
                tbl = 'ein_auspendler_zusammengefasst'
                df = pd.read_sql(f'SELECT * FROM {self.schema}.{tbl}', conn)
                df.to_excel(excel_writer=excel_writer,
                            sheet_name='zusammengefasst')

                tbl = 'ein_auspendler'
                df = pd.read_sql(f'SELECT * FROM {self.schema}.{tbl}', conn)
                df.to_excel(excel_writer=excel_writer, sheet_name='Rohdaten')

