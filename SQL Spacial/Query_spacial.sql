---------- PAGINA 1 ----------
-- 1.
select *
from provincias
WHERE NAME = 'Cartago';

-- 2.
select *
from ciudades;

-- 3.
select c.*
from ciudades c, provincias p
where p.id = nvl(:P1_PROV, p.id) and
      sdo_contains(p.geometry, c.shape) = true;

-- 4.
select p.*
from provincias p
where p.id = nvl(:P1_PROV, p.id);

-- 5.
select p.name as Nombre,
    count(c.id) as "Número de ciudades"
from provincias p left join ciudades c on 
     sdo_relate(p.geometry, c.shape, 'mask=contains') = true
group by p.name
order by count(c.id) desc;


---------- PAGINA 2 ----------
-- 6.
select *
from ciudades
order by nombre;

-- 7.
select *
from sismos;

-- 8.
select *
from sismos s, ciudades c
where c.id = 21 and 
      sdo_within_distance(c.shape, s.shape, 'distance = 20 units=km') = true;


---------- PAGINA 3 ----------
-- 9.
select s.*
from sismos s, provincias p
where p.id = 'CRC' and
      sdo_contains(p.geometry, s.shape) = true;

-- 10.
select s.*
from sismos s, provincias p
where p.id = 'CRC' and
      sdo_contains(p.geometry, s.shape) = true and
      s.magnitud > 5.0;

---------- PAGINA 4 ----------
-- 11.
select s.*
from sismos s
where not exists (
    select *
    from provincias prov
    where sdo_contains(prov.geometry, s.shape) = true
);

-- 12.
with prov_pacifico as (
    select *
    from provincias p
    where p.id = 'CRP' or p.id = 'CRG'
),
sismos_no_prov as (
    select s.*
    from sismos s
    where not exists (
        select *
        from provincias prov
        where sdo_contains(prov.geometry, s.shape) = true
    )
)
select s.*
from sismos_no_prov s, prov_pacifico p
where sdo_within_distance(p.geometry, s.shape, 'distance = 10 unit=km') = true;



---------- PAGINA 5 ----------
-- 13.
select *
from sismos;

-- 14.
select *
from sismos
where magnitud >= 6.0;

-- 15.
select sdo_aggr_convexhull(sdoaggrtype(shape, 0.005))
from sismos
where magnitud >= 6.0;

-- 16.
with convexhull as (
    select sdo_aggr_convexhull(sdoaggrtype(shape, 0.005)) as shape
    from sismos
    where magnitud >= 6.0
)
select sdo_geom.sdo_intersection(c.shape, p.geometry, 0.005)
from convexhull c, provincias p
where p.id = 'CRA';

-- 17.
with convexhull as (
    select sdo_aggr_convexhull(sdoaggrtype(shape, 0.005)) as shape
    from sismos
    where magnitud >= 6.0
)
select to_char(round(sdo_geom.sdo_area(sdo_geom.sdo_intersection(
       c.shape, p.geometry, 0.005), 0.005) / 1000000, 2)) || '  km2' as "Área"
from convexhull c, provincias p
where p.id = 'CRA';


















