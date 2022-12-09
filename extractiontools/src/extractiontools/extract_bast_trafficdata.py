from extractiontools.ausschnitt import Extract


class ExtractBASt(Extract):
    """
    Extract the BASt Traffic data
    """
    schema = 'bast'
    role = 'group_osm'
    tables = {'bfstr_netz_nk': 'geom',
              'bfstr_netz_np': 'geom',
              'bfstr_netz_sk': 'geom',
              #'svz_2021': None,
              }

    def final_stuff(self):

        sql = f"""
        SELECT vonnk, nachnk FROM {self.schema}.bfstr_netz_sk;
        """
        cur = self.conn.cursor()
        self.logger.debug(sql)
        cur.execute(sql)
        rows = cur.fetchall()

        rows_to_compare = [(row[0], row[1]) for row in rows]

        sql = f'''
        SELECT v.*
        INTO {self.schema}.svz_2021
        FROM {self.temp}.svz_2021 v WHERE (v.vonnk, v.nachnk) = ANY(ARRAY[{rows_to_compare}]);
        '''
        self.logger.debug(sql)
        cur.execute(sql)
        sql = f'''CREATE INDEX svz_2021_idx ON {self.schema}.svz_2021
        USING btree (vonnk, nachnk);'''
        cur.execute(sql)


        self.logger.info(f'Creating views')
        self.copy_views_to_target_db(schema=self.schema)
