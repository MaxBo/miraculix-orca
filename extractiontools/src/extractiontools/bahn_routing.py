#!/usr/bin/env python
# coding:utf-8

from extractiontools.connection import DBApp, Connection
from extractiontools.utils.bahn_query import BahnQuery
from typing import List


class DBRouting(DBApp):
    """Route"""
    tables = {}
    schema = 'timetables'

    def __init__(self,
                 database,
                 date: 'date',
                 times: List[int],
                 schema='timetables',
                 **kwargs
                 ):
        super().__init__(**kwargs)

        self.set_login(database=database)
        self.schema = schema
        self.date = date
        self.times = times
        self.db_query = BahnQuery(dt=self.date, timeout=0.5)

    def scrape(self, destination_table: str, max_distance: int=None):
        s = destination_table.split('.')
        schema, table = s if len(s) > 1 else ('public', s[0])
        target_table = f"db_fastest_{table}_{self.date.strftime('%Y_%m_%d')}"
        with Connection(login=self.login) as conn:
            self.logger.info(f'Creating target table "{target_table}" '
                             f'in schema "{schema}"')
            pk = self.get_primary_key(schema, table, conn=conn)
            sql = f'SELECT pg_typeof({pk}) FROM "{schema}"."{table}" LIMIT 1;'
            cur = conn.cursor()
            cur.execute(sql)
            pk_type = cur.fetchone()[0]
            sql = f'''
            DROP TABLE IF EXISTS "{schema}"."{target_table}";
            CREATE TABLE "{schema}"."{target_table}"
            (
              "origin_H_ID" bigint NOT NULL,
              destination_id {pk_type} NOT NULL,
              "destination_H_ID" bigint NOT NULL,
              "destination_H_NAME" text,
              modes text,
              changes integer,
              duration integer,
              departure text,
              PRIMARY KEY ("origin_H_ID", destination_id),
              CONSTRAINT "{target_table}_dest_pk" FOREIGN KEY (destination_id)
                REFERENCES "{schema}"."{table}" ({pk}) MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION,
              CONSTRAINT "{target_table}_orig_pk" FOREIGN KEY ("origin_H_ID")
                REFERENCES "{self.schema}".haltestellen ("H_ID") MATCH SIMPLE
                ON UPDATE NO ACTION ON DELETE NO ACTION
            );
            '''
            self.run_query(sql, conn=conn)
            conn.commit()

            self.logger.info('Fetching combinations of origins and destinations'
                             ' within radius')
            sql = f'''
            SELECT
            "H_ID", "H_Name", d.{pk},
            ST_X(ST_TRANSFORM(d.geom, 4326)), ST_Y(ST_TRANSFORM(d.geom, 4326))
            FROM
            "{self.schema}".haltestellen h,
            "{schema}"."{table}" d
            WHERE ST_DWithin(h.geom, d.geom, 70000);
            '''
            cur.execute(sql)
            routes = cur.fetchall()
            closest_stops = {}

            self.logger.info(f'Scraping {len(routes)} Routes '
                             'from Deutsche Bahn')
            for i, (origin_stop_id, origin_stop_name,
                 dest_id, dest_x, dest_y) in enumerate(routes):
                closest_stop = closest_stops.get(dest_id)
                if not closest_stop:
                    cs_res = self.db_query.stops_near(
                        (dest_x, dest_y), max_distance=2000)
                    closest_stop = cs_res[0] if len(cs_res) > 0 else -1
                    closest_stops[dest_id] = closest_stop
                if closest_stop == -1:
                    continue
                (duration, departure,
                 changes, modes) = self.db_query.fastest_route(
                     origin_stop_name, closest_stop['name'], self.times,
                     max_retries=5)
                if duration > 10000000:
                    continue
                if 'character' in pk_type or 'text' in pk_type:
                    dest_id = f"'{dest_id}'"
                sql = f'''
                INSERT INTO "{schema}"."{target_table}"
                ("origin_H_ID", destination_id,
                 "destination_H_ID", "destination_H_NAME",
                 modes, changes, duration, departure)
                VALUES
                ({origin_stop_id}, {dest_id},
                 {closest_stop['id']}, '{closest_stop['name']}',
                 '{modes}', {changes}, {duration}, '{departure}')
                '''
                cur.execute(sql)
                if (i + 1) % 100 == 0:
                    self.logger.info(f'{i+1}/{len(routes)} Routes processed')
                    conn.commit()