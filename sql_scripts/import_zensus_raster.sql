CREATE TABLE regionalstatistik.ew_zensus2011_gitter
(
  id text NOT NULL,
  x_mp_100m double precision,
  y_mp_100m double precision,
  einwohner integer,
  CONSTRAINT ew_zensus2011_gitter_pkey PRIMARY KEY (id)
);

COPY regionalstatistik.ew_zensus2011_gitter FROM '/home/mb/gis/laea/zensus/Zensus_Bevoelkerung_100m-Gitter.csv' DELIMITER ';' CSV HEADER ;
DELETE FROM regionalstatistik.ew_zensus2011_gitter WHERE einwohner = -1;

DROP TABLE regionalstatistik.zensus2011_gitter1000m_spitze;
CREATE TABLE regionalstatistik.zensus2011_gitter1000m_spitze
(id text primary key,
x double precision,
y double precision,
einwohner integer,
alter_d float,
unter18_a float,
ab65_a float,
auslaender_a float,
hhgroesse_d float,
leerstandsquote float,
wohnfl_bew_d float,
wohnfl_wohnung float);

COPY regionalstatistik.zensus2011_gitter1000m_spitze FROM '/home/mb/gis/laea/zensus/Zensus_spitze.csv' DELIMITER ';' CSV HEADER ;
VACUUM ANALYZE regionalstatistik.zensus2011_gitter1000m_spitze;

DROP TABLE regionalstatistik.zensus2011_gitter1000m_klass;
CREATE TABLE regionalstatistik.zensus2011_gitter1000m_klass
(id text primary key,
x double precision,
y double precision,
einwohner integer,
frauen_a float,
alter_d float,
unter18_a float,
ab65_a float,
auslaender_a float,
hhgroesse_d float,
leerstandsquote float,
wohnfl_bew_d float,
wohnfl_wohnung float);
COPY regionalstatistik.zensus2011_gitter1000m_klass FROM '/home/mb/gis/laea/zensus/Zensus_klassifiziert.csv' DELIMITER ';' CSV HEADER ;
VACUUM ANALYZE regionalstatistik.zensus2011_gitter1000m_klass;