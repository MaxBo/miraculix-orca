from extractiontools.connection import Login, Connection, DBApp
import requests
import json


class Destatis(DBApp):
    schema = 'destatis'
    url = f'https://www-genesis.destatis.de/genesisWS/rest/2020'
    params = {
        'language': 'de',
        'category': 'all',
        #'username': '',
        #'password': '',
        #'pagelength': 16
    }
    special_chars = 'äüö!@#$%^&*()[]{};:,./<>?\|`~=+"\' '
    tablename_length = 63

    def __init__(self, database: str, **kwargs):
        """"""
        super().__init__(schema=self.schema, **kwargs)
        self.set_login(database=database)
        sql = f'''
        CREATE SCHEMA IF NOT EXISTS {self.schema};
        '''
        with Connection(login=self.login) as conn:
            self.run_query(sql, conn=conn, verbose=False)

    def find(self, search_term: str, category: str='all'):
        self.logger.info(f'Querying search term "{search_term}" in '
                         f'category "{category}"')
        params = self.params.copy()
        params['term'] = search_term
        params['category'] = category
        url = f'{self.url}/find/find'
        retries = 0
        res = None
        while retries < 3:
            try:
                res = requests.get(url, params=params)
                break
            except ConnectionError:
                retries += 1
        if not res:
            raise ConnectionError('Destatis is not responding.')
        if res.status_code != 200:
            self.logger.info(f'Destatis Error {res.status_code}. Skipping...')
            return
        return res.json()

    def add_table_codes(self, search_term: str):
        with Connection(login=self.login) as conn:
            sql = f'''
            CREATE TABLE IF NOT EXISTS {self.schema}.table_codes (
            code varchar(20) PRIMARY KEY,
            content varchar(256) NOT NULL,
            tablename varchar({self.tablename_length}) NOT NULL
            )
            '''
            self.run_query(sql, conn=conn, verbose=False)
            res = self.find(search_term.strip(), category='tables')
            if not res:
                return
            tables = res['Tables'] or []
            self.logger.info(f'Found {len(tables)} tables.')
            rem = {ord(c): "" for c in self.special_chars}
            for table in tables:
                code = table['Code']
                content = table['Content'][:256].replace('\n', ' ')
                table_name = f'{code}_{content.lower()}'.translate(rem)
                table_name = table_name[:self.tablename_length]
                sql = f'''
                INSERT INTO {self.schema}.table_codes
                (code, content, tablename)
                VALUES ('{code}','{content}','{table_name}')
                ON CONFLICT (code)
                DO
                UPDATE SET content='{content}', tablename='{table_name}';
                '''
                self.run_query(sql, conn=conn, verbose=False)

    def get_tables(self):
        sql = f'SELECT * FROM {self.schema}.table_codes ORDER BY code ASC;'
        with Connection(login=self.login) as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            return rows