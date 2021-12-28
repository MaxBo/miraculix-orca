import os
import shutil
import datetime
from typing import List, Dict, Tuple
import pandas as pd
import numpy as np
import tempfile
from extractiontools.connection import Connection, DBApp, Login


class ImportPendlerdaten(DBApp):
    """
    Import Commuter trips from excel-files to database
    """
    schema = 'pendler'
    role = 'group_osm'

    def __init__(self,
                 db: str = 'extract',
                 subfolder: str = 'Pendlerdaten',
                 pendlerdaten_years: List[int] = [2020],
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
        for folder, dirs, files in os.walk(path):
            year = os.path.split(folder)[-1]
            try:
                year = int(year)
            except ValueError:
                continue
            if year not in self.pendlerdaten_years:
                continue
            for file in files:
                if not os.path.splitext(file)[1] in ['.xlsb']:
                    continue
                self.process_file(os.path.join(folder, file))

    def process_file(self, filepath: str):
        """upload a single excel-file with Pendlerdaten"""
        print(filepath)

        data_cols = ['insgesamt', 'Männer', 'Frauen', 'Deutsche', 'Ausländer', 'Azubis']

        dtype = {'ags_wo': np.unicode_, 'ags_ao': np.unicode_, }

        na_values = dict()
        for col in data_cols:
            na_values[col] = ['*']

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

        df = df_auspendler.append(df_einpendler)
        tablename = 'pendlerdaten.ein_auspendler'

        try:
            file_name = tempfile.mktemp(suffix='.csv')
            print(file_name)
            df.to_csv(file_name, encoding='UTF8')
            cur = self.conn.cursor()
            delete_sql = f'DELETE FROM {tablename} WHERE "Bundesland" = %s AND "Stichtag" = %s'
            bundesland, stichtag = df.reset_index().loc[1, ['Bundesland', 'Stichtag']]
            cur.execute(delete_sql, (bundesland, stichtag))
            print(f'deleted {cur.rowcount} rows in {tablename}')
            with open(file_name, encoding='UTF8') as file:
                cur.copy_expert("COPY pendlerdaten.ein_auspendler FROM STDIN WITH CSV HEADER ENCODING 'UTF8';", file)
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
        df = pd.read_excel(filepath,
                           sheet_name=sheet_name,
                           engine='pyxlsb',
                           dtype=dtype,
                           skiprows=9,
                           header=None,
                           skipfooter=4,
                           names=cols,
                           na_values=na_values,
                           )
        df[from_cols] = df[from_cols].fillna(method='ffill')
        df = df.loc[~df[index_cols[2]].isna()]

        #  mark other counties with Ü
        others = df[index_cols[3]].str.startswith('Übrige ')
        df.loc[others, index_cols[2]] = df.loc[others, index_cols[2]] + 'Ü'
        df.drop_duplicates(keep='first', inplace=True)

        bundesland, stichtag = self.read_header(filepath, sheet_name)
        df['Bundesland'] = bundesland
        df['Stichtag'] = stichtag
        df['Ein_Aus'] = sheet_name
        df.set_index(['Bundesland', 'Ein_Aus', 'Stichtag', 'ags_wo', 'ags_ao'],
                     inplace=True)
        return df

    def read_header(self,
                    filepath: str,
                    sheet_name: str) -> Tuple[str, datetime.date]:
        # parse header
        df = pd.read_excel(filepath,
                           sheet_name=sheet_name,
                           engine='pyxlsb',
                           skiprows=lambda x: x > 5
                           )
        bundesland = df.iloc[2, 0]
        stichtag = df.iloc[3, 0].split(': ')[-1]
        stichtag = datetime.datetime.strptime(stichtag, "%d.%m.%Y")
        return bundesland, stichtag
