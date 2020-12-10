
create table vehicles (
  vin text primary key,
  created_on timestamp default now(),
  make text not null,
  model text not null,
  version text,
  year integer not null
)

create table vehicle_listings (
  id serial primary key,
  created_on timestamp not null default now(),
  vin text references vehicles (vin),
  source text not null,
  owner text not null,
  zip integer not null,
  mileage integer not null,
  price integer not null,
  title text,
  unique (vin, created_on)
)