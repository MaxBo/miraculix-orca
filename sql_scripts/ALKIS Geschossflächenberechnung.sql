UPDATE geobasisdaten.alkis_gebaeude_bauteile g
SET geom = st_multi((n.d).geom)

--SELECT g.objectid, st_area((n.d).geom)

FROM 
--geobasisdaten.alkis_gebaeude_bauteile g,
(
SELECT g.objectid, st_dump(st_makevalid(geom)) AS d
FROM geobasisdaten.alkis_gebaeude_bauteile g
WHERE NOT st_isvalid(geom) LIMIT 10) n
WHERE n.objectid = g.objectid
AND (n.d).path[1] = 1;

SELECT 
st_astext(geom), st_astext(st_makevalid(geom))
FROM geobasisdaten.alkis_gebaeude_bauteile b
WHERE NOT st_isvalid(geom);


 SELECT g.objectid,st_difference(g.geom, i.geom) AS geom

 FROM
 geobasisdaten.alkis_gebaeude_bauteile g,
 (
 SELECT
 g.objectid, st_union(b.geom) AS geom, sum(st_area(b.geom)) AS sum_inner_areas
 FROM

 geobasisdaten.alkis_gebaeude_bauteile g,
 geobasisdaten.alkis_gebaeude_bauteile b
 WHERE g.kenn = 'AX31001'
 AND b.kenn = 'AX31002'
 --AND g.objectid = 198795
 AND st_intersects(g.geom, b.geom)
 GROUP BY g.objectid) i
 WHERE i.objectid = g.objectid;

 SELECT kenn, count(*)
 FROM  geobasisdaten.alkis_gebaeude_bauteile
 GROUP BY kenn;
SELECT max(hho) 
FROM  geobasisdaten.alkis_gebaeude_bauteile;
 


CREATE TABLE geobasisdaten.alkis_gebaeude (  
  objectid integer NOT NULL,
  gfk smallint,
  bezgfk character varying(64),
  wgf character varying(32),
  bezwgf character varying(254),
  ofl smallint,
  aog smallint,
  aug smallint,
  hoh character varying(5),
  bat smallint,
  nam character varying(254),
  baw smallint,
  bezbaw character varying(64),
  hho double precision,
  zus smallint,
  geom geometry(MultiPolygon,3035),
  CONSTRAINT alkis_gebaeude_pkey PRIMARY KEY (objectid));


TRUNCATE geobasisdaten.alkis_gebaeude;
-- Hauptgebäudeteil von Gebäuden mit Bauteilen
INSERT INTO geobasisdaten.alkis_gebaeude (
 objectid,
 -- Gebäudefunktion
 gfk, bezgfk,
 -- weitere Gebäudefunktiion
 wgf, bezwgf,
 -- Lage zur Oberfläche, anzahl Geschosse, Hochhaus
 ofl, aog, aug, hoh,
 -- Name, Bauweise
 nam, baw, bezbaw,
 -- Zustand 
 zus, 
 geom)

SELECT 
d.objectid, 
d.gfk, d.bezgfk, 
d.wgf, d.bezwgf, 
d.ofl, d.aog, d.aug, d.hoh,
d.nam, d.baw, d.bezbaw,
d.zus,
CASE WHEN st_npoints(d.geom) = 0
THEN NULL
ELSE d.geom 
END AS geom
FROM (

SELECT g.objectid, 
g.gfk, g.bezgfk, 
g.wgf, g.bezwgf, 
g.ofl, g.aog, g.aug, g.hoh,
g.nam, g.baw, g.bezbaw,
g.zus,
st_multi(st_difference(g.geom, i.geom)) AS geom

 FROM
 geobasisdaten.alkis_gebaeude_bauteile g,
 (
 SELECT
 g.objectid, st_union(b.geom) AS geom
 FROM

 geobasisdaten.alkis_gebaeude_bauteile g,
 geobasisdaten.alkis_gebaeude_bauteile b 
LEFT JOIN geobasisdaten.geschossanrechnung a
ON b.bat = a.bat
 WHERE g.kenn = 'AX31001'
 AND b.kenn = 'AX31002'
 -- nur die Bauteile abziehen, die selber eine Anzahl Obergeschosse definiert haben,
 -- oder die eine positive Default-Anzahl von Obergeschossen (insb. auskragende Bauteile) definiert haben
 AND COALESCE(b.aog, COALESCE(a.aog_default, 0)) >= 0
 AND st_intersects(g.geom, b.geom)
-- AND g.objectid = 198303
 GROUP BY g.objectid) i
 WHERE i.objectid = g.objectid
 ) d
;
 
-- Bauteile
INSERT INTO geobasisdaten.alkis_gebaeude (
 objectid,
 -- Gebäudefunktion
 gfk, bezgfk,
 -- weitere Gebäudefunktiion
 wgf, bezwgf,
 -- Lage zur Oberfläche, anzahl Geschosse, Hochhaus
 ofl, aog, aug, hoh,
 -- Name, Bauweise
 nam, baw, bezbaw,
 -- Bauteil, Zustand 
 bat, zus, 
 geom)
 
SELECT 
--g.objectid gid, 
b.objectid, 
g.gfk, g.bezgfk, 
b.wgf, b.bezwgf, 
b.ofl, COALESCE(b.aog, g.aog * a.aog_factor) AS aog, 
b.aug, g.hoh,
g.nam, b.baw, b.bezbaw,
b.bat, g.zus,
b.geom AS geom

FROM(
SELECT g.objectid gid, 
b.objectid,
row_number() OVER(PARTITION BY b.objectid ORDER BY g.aog DESC NULLS LAST) AS rn
 FROM
 geobasisdaten.alkis_gebaeude_bauteile g,
 geobasisdaten.alkis_gebaeude_bauteile b
 WHERE g.kenn = 'AX31001'
 --AND g.aog > 0
 AND b.kenn = 'AX31002'
 AND st_within(b.geom, g.geom)
 --AND g.objectid = 198303

 ) gb,
 geobasisdaten.alkis_gebaeude_bauteile g,
 geobasisdaten.alkis_gebaeude_bauteile b
 LEFT JOIN geobasisdaten.geschossanrechnung a
 ON b.bat = a.bat
 WHERE gb.rn = 1
 AND g.objectid = gb.gid
 AND b.objectid = gb.objectid;


-- Gebäude ohne weitere Bauteile
INSERT INTO geobasisdaten.alkis_gebaeude (
 objectid,
 -- Gebäudefunktion
 gfk, bezgfk,
 -- weitere Gebäudefunktiion
 wgf, bezwgf,
 -- Lage zur Oberfläche, anzahl Geschosse, Hochhaus
 ofl, aog, aug, hoh,
 -- Name, Bauweise
 nam, baw, bezbaw,
 -- Zustand 
 zus, 
 geom)
SELECT g.objectid, 
g.gfk, g.bezgfk, 
g.wgf, g.bezwgf, 
g.ofl, g.aog, g.aug, g.hoh,
g.nam, g.baw, g.bezbaw,
g.zus,
g.geom AS geom

 FROM
 geobasisdaten.alkis_gebaeude_bauteile g
 WHERE g.kenn = 'AX31001'
 AND NOT EXISTS (
 SELECT 1 FROM geobasisdaten.alkis_gebaeude b
 WHERE g.objectid = b.objectid);

REFRESH MATERIALIZED VIEW dichte_km2.geschossflaeche;
REFRESH MATERIALIZED VIEW dichte_ha.geschossflaeche;

DROP MATERIALIZED VIEW dichte_km2.geschossflaeche CASCADE;
CREATE MATERIALIZED VIEW dichte_km2.geschossflaeche AS
SELECT 
l.cellcode,
sum(
  st_area(st_intersection(g.geom, l.geom)) * 
  -- ggf. Defaultwert nehmen (
  coalesce(g.aog, a.aog_default)) 
  as total_floorspace
FROM geobasisdaten.alkis_gebaeude g 
LEFT JOIN geobasisdaten.geschossanrechnung a
ON g.bat = a.bat,
laea.laea_vector_1000 l
WHERE l.geom && g.geom
GROUP BY l.cellcode;
CREATE INDEX geschossflaeche_pkey ON
dichte_km2.geschossflaeche USING btree(cellcode);

ANALYZE dichte_km2.geschossflaeche;
CREATE OR REPLACE VIEW dichte_km2.geschossflaeche_raster AS
SELECT
v.cellcode, v.total_floorspace, l.geom,
row_number() OVER(ORDER BY v.cellcode)::integer AS rn
FROM
dichte_km2.geschossflaeche v,
laea.laea_vector_1000 l
WHERE v.cellcode=l.cellcode;


DROP MATERIALIZED VIEW dichte_ha.geschossflaeche CASCADE;
CREATE MATERIALIZED VIEW dichte_ha.geschossflaeche AS
SELECT 
l.cellcode,
sum(st_area(st_intersection(g.geom, l.geom)) * coalesce(g.aog, a.aog_default)) as total_floorspace
FROM geobasisdaten.alkis_gebaeude g 
LEFT JOIN geobasisdaten.geschossanrechnung a
ON g.bat = a.bat,
laea.laea_vector_100 l
WHERE l.geom && g.geom
GROUP BY l.cellcode;
CREATE INDEX geschossflaeche_pkey ON
dichte_ha.geschossflaeche USING btree(cellcode);

ANALYZE dichte_ha.geschossflaeche;
CREATE OR REPLACE VIEW dichte_ha.geschossflaeche_raster AS
SELECT
v.cellcode, v.total_floorspace, l.geom,
row_number() OVER(ORDER BY v.cellcode)::integer AS rn
FROM
dichte_ha.geschossflaeche v,
laea.laea_vector_100 l
WHERE v.cellcode=l.cellcode;
