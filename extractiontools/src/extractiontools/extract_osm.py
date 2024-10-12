#!/usr/bin/env python
# coding:utf-8

from argparse import ArgumentParser

from extractiontools.ausschnitt import Extract
import psycopg2


class ExtractOSM(Extract):
    """
    Extract the osm data
    """
    schema = 'osm'
    role = 'group_osm'

    def additional_stuff(self):
        """
        """
        self.set_session()
        try:
            self.extract_nodes()
            self.extract_ways()
            self.copy_way_nodes()
            self.copy_relations()
            self.conn.commit()
            self.copy_users()
            self.copy_schema_info()
            self.add_comments()
        except Exception as e:
            self.conn.rollback()
            raise(e)
        finally:
            self.remove_session()

    def get_timestamp(self):
        sql = f'''
        SELECT max(latest_timestamp) FROM {self.temp}.replication_changes;
        '''
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchone().max

    def add_comments(self):
        self.logger.info('Applying table descriptions')
        timestamp = self.get_timestamp()
        t = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        for table in ['relation_members', 'users', 'way_nodes', 'relations',
                      'ways', 'nodes', 'actions']:
            description = self.get_description(table, self.schema,
                                               foreign=True) or ''
            description = f'timestamp: {t} \r\n{description}'
            sql = f"COMMENT ON TABLE {self.schema}.{table} IS '{description}'"
            self.run_query(sql)

    def copy_relations(self):
        """
        copy relation and relation_member Schema
        """
        self.logger.info(f'Copying relation members to '
                         f'{self.schema}.relation_members')
        sql = f"""
        CREATE TABLE "{self.schema}".relations AS
        (SELECT * FROM {self.temp}.relations) WITH NO DATA;
        """
        self.run_query(sql, conn=self.conn)
        sql = f"""
        CREATE TABLE "{self.schema}".relation_members AS
        (SELECT * FROM {self.temp}.relation_members) WITH NO DATA;
        """
        self.run_query(sql, conn=self.conn)
        self.conn.commit()

        self.logger.info(f'Copying relations to {self.schema}.relations')

        if len(self.way_ids) == 0:
            return
        relation_ids = {}
        chunksize = 10000
        cur = self.conn.cursor()
        for i in range(0, len(self.way_ids), chunksize):
            cur_ids = self.way_ids[i: i + chunksize]
            arr = ','.join([str(ci) for ci in cur_ids])
            sql = f"""
            -- get relation_ids for ways
            SELECT rm.relation_id
            FROM
              {self.temp}.relation_members rm
            WHERE
              rm.member_id = ANY(ARRAY[{arr}]) AND
              rm.member_type = 'W'::bpchar
              )
            ;"""
            cur.execute(sql)
            rows = cur.fetchall()
            relations_ids.add({row[0] for row in rows})

        sql = f'''
        SELECT id FROM {self.schema}.nodes n;
        '''
        cur.execute(sql)
        rows = cur.fetchall()
        self.node_ids = [row[0] for row in rows]

        for i in range(0, len(self.way_ids), chunksize):
            cur_ids = self.way_ids[i: i + chunksize]
            arr = ','.join([str(ci) for ci in cur_ids])
            sql = f"""
            -- get relation_ids for nodes
            SELECT rm.relation_id
            FROM
              {self.temp}.relation_members rm
            WHERE
              rm.member_id = ANY(ARRAY[{arr}]) AND
              rm.member_type = 'N'::bpchar
            ;"""
            cur.execute(sql)
            rows = cur.fetchall()
            relations_ids.add({row[0] for row in rows})

        while relations_ids:
            arr = ','.join([str(i) for i in relations_ids])
            sql = f'''
            SELECT id FROM {self.schema}.relations tr WHERE id = ANY(ARRAY[{arr}]);
            '''
            self.logger.debug(sql)
            cur.execute(sql)
            rows = cur.fetchall()
            already_in = {row[0] for row in rows}
            relations_ids -= already_in
            if not relations_ids:
                break
            arr = ','.join([str(i) for i in relations_ids])

            sql = f'''
            INSERT INTO {self.schema}.relations
            SELECT id, version, user_id, tstamp, changeset_id, tags
            FROM {self.temp}.relations WHERE id = ANY(ARRAY[{arr}])
            '''
            self.logger.debug(sql)
            cur.execute(sql)

            sql = f'''
            SELECT DISTINCT rm.relation_id AS id
            FROM {self.temp}.relation_members rm
            WHERE rm.member_id = ANY(ARRAY[{arr}])
            AND rm.member_type = 'R';
            '''
            self.logger.debug(sql)
            cur.execute(sql)
            rows = cur.fetchall()
            relations_ids = {row[0] for row in rows}

        sql = f'''
        SELECT id FROM {self.schema}.relations r;
        '''
        cur.execute(sql)
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        if len(ids) == 0:
            return
        chunksize = 10000
        for i in range(0, len(ids), chunksize):
            cur_ids = ids[i: i + chunksize]
            arr = ','.join([str(ci) for ci in cur_ids])
            sql = f"""
            -- INSERT Relation members
            INSERT INTO {self.schema}.relation_members
            SELECT rm.*
            FROM {self.temp}.relation_members rm
            WHERE rm.relation_id = ANY(ARRAY[{arr}]);
            """
            self.run_query(sql, conn=self.conn)

    def copy_way_nodes(self):
        """
        copy the way_nodes in that area
        """

        self.logger.info(f'Copying way nodes to {self.schema}.way_nodes')

        sql = f'''
        CREATE TABLE {self.schema}.way_nodes AS
        (SELECT * FROM {self.temp}.way_nodes) WITH NO DATA;
        '''
        self.run_query(sql, conn=self.conn)

        sql = f'''
        SELECT id FROM {self.schema}.ways w;
        '''
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        self.way_ids = [row[0] for row in rows]
        if len(self.way_ids) == 0:
            return
        chunksize = 10000
        for i in range(0, len(self.way_ids), chunksize):
            cur_ids = self.way_ids[i: i + chunksize]
            arr = ','.join([str(ci) for ci in cur_ids])
            sql = f"""
            -- INSERT way_nodes
            INSERT INTO {self.schema}.way_nodes
            SELECT wn.*
            FROM {self.temp}.way_nodes wn
            WHERE wn.way_id = ANY(ARRAY[{arr}]);
            """
            self.run_query(sql, conn=self.conn)

        sql = '''
        SELECT DISTINCT wn.node_id FROM "{schema}".way_nodes wn
        WHERE NOT EXISTS (SELECT 1 FROM "{schema}".nodes tn WHERE wn.node_id = tn.id);
        '''.format(temp=self.temp, schema=self.schema)

        cur.execute(sql)
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        arr = ','.join([str(id) for id in ids])

        self.logger.info(f'Copying related nodes to {self.schema}.nodes')
        sql = f'''
        INSERT INTO {self.schema}.nodes
        SELECT
          n.id, n.version, n.user_id, n.tstamp, n.changeset_id, n.tags,
          st_transform(n.geom, {self.target_srid}) AS geom
        FROM {self.temp}.nodes n
        WHERE n.id = ANY(ARRAY[{arr}]);
        '''
        self.run_query(sql, conn=self.conn)

    def copy_users(self):
        """
        copy users
        """
        sql = f"""
        SELECT DISTINCT a.user_id FROM
        (SELECT user_id FROM {self.schema}.nodes
        UNION
        SELECT user_id FROM {self.schema}.ways
        UNION
        SELECT user_id FROM {self.schema}.relations) a;
        """
        self.logger.info(f'Copying OSM users to {self.schema}.users')
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        if len(ids) == 0:
            return
        chunksize = 1000

        sql = f'''
        CREATE TABLE {self.schema}.users
        (id INTEGER NOT NULL,
        name TEXT NOT NULL);'''
        self.run_query(sql, conn=self.conn)

        for i in range(0, len(ids), chunksize):
            cur_ids = ids[i: i + chunksize]
            arr = ','.join([str(ci) for ci in cur_ids])
            sql = f"""
            INSERT
            INTO {self.schema}.users
            (id, name)
            SELECT id, name
            FROM {self.temp}.users
            WHERE id = ANY(ARRAY[{arr}]);
            """
            self.run_query(sql, conn=self.conn)

    def set_session(self):
        '''
        set session geometry
        '''
        self.wkt = self.get_target_boundary()
        self.temp_meta = 'meta_temp'
        self.create_foreign_schema(foreign_schema='meta',
                                   target_schema=self.temp_meta)
        sql = f'''
        INSERT INTO {self.temp_meta}.session_boundary (session_id, source_geom, target_srid)
        VALUES
        ('{self.session_id}', st_transform(ST_GeomFromEWKT('SRID={self.srid};{self.wkt}'), 4326), {self.target_srid});
        ANALYZE {self.temp_meta}.session_boundary;
        '''
        self.logger.info('Creating session')
        self.run_query(sql, conn=self.conn)

    def remove_session(self):
        '''
        remove session geometry
        '''
        sql = f'''
        DELETE FROM {self.temp_meta}.session_boundary
        WHERE session_id='{self.session_id}'
        '''
        self.logger.info('Removing session')
        try:
            self.run_query(sql, conn=self.conn)
        # table not created yet
        except psycopg2.errors.UndefinedTable:
            pass
        self.cleanup(self.temp_meta)

    def extract_ways(self):
        """
        """
        sql = """
        SELECT
          w.id, w.version, w.user_id, w.tstamp, w.changeset_id, w.tags, w.nodes,
          st_transform(st_setsrid(Box2D(w.linestring), {source_srid}), {target_srid})::geometry(GEOMETRY, {target_srid}) AS bbox,
          st_transform(w.linestring, {target_srid})::geometry(LINESTRING, {target_srid}) AS linestring
        INTO "{schema}".ways
        FROM "{temp_meta}".osm_ways w
        WHERE w.session_id='{session_id}';
        ANALYZE "{schema}".ways;
        """
        self.logger.info(f'Extracting ways into {self.schema}.ways')
        self.run_query(sql.format(temp_meta=self.temp_meta, schema=self.schema,
                                  target_srid=self.target_srid,
                                  source_srid=self.srid,
                                  session_id=self.session_id),
                       conn=self.conn)

    def extract_nodes(self):
        """
        """
        sql = """
        SELECT
          n.id, n.version, n.user_id, n.tstamp, n.changeset_id, n.tags,
          st_transform(n.geom, {target_srid})::geometry('POINT', {target_srid}) AS geom
        INTO "{schema}".nodes
        FROM {temp_meta}.osm_nodes n
        WHERE n.session_id='{session_id}';
        ANALYZE "{schema}".nodes;
        """
        self.logger.info(f'Extracting nodes into {self.schema}.nodes')
        self.run_query(sql.format(temp=self.temp, schema=self.schema,
                                  target_srid=self.target_srid,
                                  temp_meta=self.temp_meta,
                                  session_id=self.session_id),
                       conn=self.conn)

    def copy_schema_info(self):
        """
        copy schema and actions
        """
        sql = """
        SELECT *
        INTO "{schema}".actions
        FROM {temp}.actions;

        SELECT *
        INTO "{schema}".schema_info
        FROM {temp}.schema_info;
        """.format(temp=self.temp, schema=self.schema)
        self.logger.info('Copying schema info')
        self.run_query(sql, self.conn)

    def create_index(self):
        """
        CREATE INDEX
        """
        sql = """
        ALTER TABLE "{schema}".actions ADD PRIMARY KEY (data_type, id);
        ALTER TABLE "{schema}".nodes ADD PRIMARY KEY (id);
        CREATE INDEX idx_nodes_geom
          ON "{schema}".nodes
          USING gist
          (geom);
        ALTER TABLE "{schema}".nodes CLUSTER ON idx_nodes_geom;
        CLUSTER "{schema}".nodes;
        ALTER TABLE "{schema}".relation_members ADD PRIMARY KEY (relation_id, sequence_id);
        CREATE INDEX idx_relation_members_member_id_and_type
          ON "{schema}".relation_members
          USING btree
          (member_id, member_type COLLATE pg_catalog."default");
        ALTER TABLE "{schema}".relations ADD PRIMARY KEY (id);
        ALTER TABLE "{schema}".schema_info ADD PRIMARY KEY (version);
        ALTER TABLE "{schema}".users ADD PRIMARY KEY (id);
        ALTER TABLE "{schema}".way_nodes ADD PRIMARY KEY (way_id, sequence_id);
        CREATE INDEX idx_way_nodes_node_id
          ON "{schema}".way_nodes
          USING btree
          (node_id);
        ANALYZE osm.way_nodes;
        ALTER TABLE "{schema}".ways ADD PRIMARY KEY (id);
        CREATE INDEX idx_ways_bbox
          ON "{schema}".ways
          USING gist
          (bbox);

        CREATE INDEX idx_ways_linestring
          ON "{schema}".ways
          USING gist
          (linestring);
        ALTER TABLE "{schema}".ways CLUSTER ON idx_ways_linestring;
        CLUSTER "{schema}".ways;

        CREATE INDEX way_tags_idx
        ON osm.ways
        USING gist(tags);

        -- Partial index for nodes
        CREATE INDEX node_tags_idx
        ON osm.nodes
        USING gist(tags)
        WHERE tags <> ''::hstore;

        CREATE INDEX relations_tags_idx
        ON osm.relations
        USING gist(tags);
        ANALYZE osm.relations;

        ALTER TABLE osm.way_nodes
           ALTER COLUMN way_id
           SET (n_distinct=-0.1);

        ALTER TABLE osm.way_nodes
           ALTER COLUMN node_id
           SET (n_distinct=-0.75);

        ALTER TABLE osm.nodes
           ALTER COLUMN tags
           SET (n_distinct=-0.1);

        ALTER TABLE osm.ways
           ALTER COLUMN tags
           SET (n_distinct=-0.1);

        ALTER TABLE osm.relations
           ALTER COLUMN tags
           SET (n_distinct=-0.1);

        """.format(schema=self.schema)
        self.logger.info('Creating indexes')
        self.run_query(sql, self.conn)

    def further_stuff(self):
        """
        Copy the osm classifications and osm-view in wgs84
        to destination database
        """
        self.copy_tables_to_target_db(schema='classifications', skip_views=False)


if __name__ == '__main__':

    parser = ArgumentParser(description="Extract Data for Model")

    parser.add_argument("-n", '--name', action="store",
                        help="Name of destination database",
                        dest="destination_db", default='extract')

    parser.add_argument('--host', action="store",
                        help="host",
                        dest="host", default='gis.ggr-planung.de')
    parser.add_argument("-p", '--port', action="store",
                        help="port", type=int,
                        dest="port", default=5432)

    parser.add_argument("-U", '--user', action="store",
                        help="database user",
                        dest="user", default='osm')
    parser.add_argument('--source-db', action="store",
                        help="source database",
                        dest="source_db", default='europe')

    options = parser.parse_args()

    extract = ExtractOSM(source_db=options.source_db,
                         destination_db=options.destination_db)
    extract.set_login(host=options.host,
                      port=options.port,
                      user=options.user)
    extract.extract()
