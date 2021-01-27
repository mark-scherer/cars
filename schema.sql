-- NOTES
  -- autolist returns many cars priced at $0
    -- brief manual inspection shows no price listed in UI
    -- further, many are not even the listed model
    -->> model of ALL autolist listings is UNCONFIRMED

create table vehicles (
  vin text primary key,
  created_on timestamp default now(),
  make text not null,
  model text not null,
  version text,
  year integer not null,

  -- augmented columns: added post scraping
  -- some of these should be in vehicle_listings but whatever doesn't matter anymore (1/14/2021)
  model_validated_on timestamp,
  drivetrain text,
  color text,
  estimated_value integer,
  _owner text,
  distance int,

  -- manually added columns
  score_adjustments jsonb default '{}'::jsonb,
  sold boolean not null default false
)

create table vehicle_listings (
  id serial primary key,
  created_on timestamp not null default now(),
  vin text references vehicles (vin),
  source text not null,
  owner text,
  zip integer not null,
  mileage integer not null,
  price integer not null,
  title text,
  unique (vin, created_on)
)

-- see recent scrape results by date, model
select listings.created_on::date, model, count(*) 
from vehicles join vehicle_listings listings on vehicles.vin = listings.vin
where listings.created_on::date > now()::date - interval '2 weeks'
group by listings.created_on::date, model order by listings.created_on::date desc, model

-- see recent scrape results by date, source & model
select listings.created_on::date, source, model, count(*) 
from vehicles join vehicle_listings listings on vehicles.vin = listings.vin
where listings.created_on::date > now()::date - interval '3 days'
group by listings.created_on::date, source, model order by listings.created_on::date desc, source, model

-- see vehicles not covered by specified sources
with vehicle_sources as (
  select vehicles.vin, array_agg(distinct listings.source order by listings.source) as all_sources
  from vehicles join vehicle_listings listings
    on vehicles.vin = listings.vin
  group by vehicles.vin
) select all_sources, count(*)
from vehicle_sources
where ARRAY['autolist', 'edmunds'] && all_sources = false
group by all_sources order by count(*) desc

-- update all listings of a given color
update vehicles set
  color = 'red'
where color = 'maroon'

-- add new score_adjustments
update vehicles set
  score_adjustments = score_adjustments || jsonb_build_object('light_damages', 500)
where vin = '3C4NJDBB2JT284423'

-- set sold
update vehicles set
  sold = true
where vin = '3C4NJDBB2JT150267'

-- vehicles by number of distinct sources
with distinct_sources as (
  select array_agg(distinct source) as sources
  from vehicles join vehicle_listings listings on vehicles.vin = listings.vin
  group by vehicles.vin
) select array_length(sources, 1) as sources, count(*)
from distinct_sources
group by array_length(sources, 1) order by array_length(sources, 1) desc

-- average length of detection
with detection_lengths as (
  select max(created_on) - min(created_on) as detection_length
  from vehicle_listings
  group by vin
) select avg(detection_length) as detection_length
from detection_lengths

-- db stats (1/26/2021)
  -- scraped 3k individual vehciles (7 different Jeep models)
  -- 57k listings
    -- across 10 weeks
    -- from 5 different sites
    -- avg 19 listings/vehicle
    -- avg vehicle was detected for 14 days
  -- listing counts by source
    -- autolist     : 48k
    -- auto_trader  :  6k
    -- edmunds      :  2k
    -- cars.com     : <1k
    -- carvana      : <1k
  -- vehicle count by number of sources
    -- 5 distinct sources   :    1 vehicle
    -- 4 distinct sources   :   32 vehicles
    -- 3 distinct sources   :  110 vehicles
    -- 2 distinct sources   :  235 vehicles
    -- 1 distinct sources   : 2.6k vehicles
