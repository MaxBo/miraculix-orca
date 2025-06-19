from extractiontools.connection import DBApp, Connection

class Gebietsstaende(DBApp):
    schema = 'verwaltungsgrenzen'
    target_schema = 'gebietsstaende'

    def __init__(self,
                 destination_db: str,
                 ref_table: str,
                 comp_tables: list[str],
                 threshold: int,
                 **kwargs):
        super().__init__(schema=self.schema, **kwargs)
        self.db = destination_db
        self.set_login(database=destination_db)
        self.ref_table = ref_table
        self.comp_tables = comp_tables
        self.threshold = threshold

    def calc(self):
        self.logger.info(f'Gleiche Gebietsstände mit Tabelle "{self.ref_table}" ab')
        with (Connection(login=self.login) as conn):
            self.create_schema(self.target_schema, conn=conn)
            target_table = f'bezug_{self.ref_table}'
            self.logger.info(f'Erstelle Zieltabelle "{self.target_schema}.{target_table}"')
            create_sql = f"""        
                DROP TABLE IF EXISTS {self.target_schema}.{target_table};
                
                CREATE table {self.target_schema}.{target_table} (
                    vergleichstabelle varchar(50),
                    ags_vergleich varchar(8),
                    gen_vergleich varchar(100),
                    flaechenanteil double precision,
                    ags_bezug varchar(8),
                    gen_bezug varchar(100),
                    area_schnitt double precision);
            """
            cur = conn.cursor()
            cur.execute(create_sql)

            ref_cols = self.get_columns(self.schema, self.ref_table, conn=conn)
            # prefer field rs over ags over key as identifier
            ref_ags = 'rs' if 'rs' in ref_cols else 'ags' if 'ags' in ref_cols else 'key'

            for comp_table in self.comp_tables:
                self.logger.info(f'Verschneide Tabelle "{self.schema}.{comp_table}"')
                comp_cols = self.get_columns(self.schema, comp_table, conn=conn)
                comp_ags = 'rs' if 'rs' in comp_cols else 'ags' if 'ags' in comp_cols else 'key'
                ref_where = f"WHERE gf = 4" if 'gf' in ref_cols else ''
                comp_where = f"WHERE gf = 4" if 'gf' in comp_cols else ''
                # join_where_clause = ''
                # if ref_where and comp_where:
                #     join_where_clause = f'({ref_where} AND {comp_where})'

                join_sql = f"""
                    WITH schnittflaechen AS (
                      SELECT
                        referenz.{ref_ags} AS ags_referenz,
                        referenz.gen AS gen_referenz,
                        vergleich.{comp_ags} AS ags_vergleich,
                        vergleich.gen AS gen_vergleich,
                        vergleich.geom AS geom_vergleich,
                        ST_Intersection(referenz.geom, vergleich.geom) AS geom_schnitt
                      FROM
                        (SELECT * FROM {self.schema}.{self.ref_table} {ref_where}) AS referenz
                    JOIN
                        (SELECT * FROM {self.schema}.{comp_table} {comp_where}) AS vergleich
                        ON ST_Intersects(referenz.geom, vergleich.geom)  
                    )
                    INSERT INTO {self.target_schema}.{target_table}
                    SELECT DISTINCT ON (ags_vergleich) *
                    FROM (
                        SELECT
                          '{comp_table}' AS vergleichstabelle,
                          RPAD(ags_vergleich, 8, '0') AS ags_vergleich,
                          gen_vergleich AS gen_vergleich,
                          ST_Area(geom_schnitt) / ST_Area(geom_vergleich) AS flaechenanteil,
                          RPAD(ags_referenz, 8, '0') AS ags_bezug,
                          gen_referenz AS gen_bezug,
                          ST_Area(geom_schnitt) AS area_schnitt
                        FROM
                          schnittflaechen
                        WHERE
                          ST_Area(geom_schnitt) > {self.threshold}
                    ) ORDER BY ags_vergleich, flaechenanteil DESC;
                """
                cur = conn.cursor()
                cur.execute(join_sql)

            comment = (f'Vergleich der Gebietsstände der Tabelle {self.ref_table} '
                       f'mit den Tabellen {", ".join(self.comp_tables)}.')
            self.set_table_comment(comment, target_table, schema=self.target_schema, conn=conn)