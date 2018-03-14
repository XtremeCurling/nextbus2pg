/*
Create the `nextbus` schema if it doesn't already exist.
*/
CREATE SCHEMA IF NOT EXISTS nextbus;

/*
Set search_path so that postGIS commands are recognized.
*/
SET search_path = public, postgis, nextbus;

/*
Create agency table.
The PK `agency_id` comes from nextbus.
*/
CREATE TABLE nextbus.agency (
	agency_id TEXT,
	name      TEXT,
	region    TEXT,
	CONSTRAINT agency_pk
		PRIMARY KEY (agency_id)
);

/*
Create route table.
The PK `route_id` is created in a Python script.
*/
-- An `agency_id` and `tag` uniquely define a route.
CREATE TABLE nextbus.route (
	route_id  UUID,
	agency_id TEXT,
	tag       TEXT,
	name      TEXT,
	CONSTRAINT route_pk
		PRIMARY KEY (route_id),
	CONSTRAINT route_belongs_to_agency_fk
		FOREIGN KEY (agency_id)
		REFERENCES nextbus.agency (agency_id),
	CONSTRAINT route_defined_by_agency_and_tag_unq
		UNIQUE (agency_id, tag)
);

/*
Create service table.
The PK `service_id` is created in a Python script.
*/
-- A `route_id` and `tag` uniquely define a service.
CREATE TABLE nextbus.service (
	service_id UUID,
	route_id   UUID,
	tag        TEXT,
	name       TEXT,
	direction  TEXT,
	use_for_ui BOOLEAN,
	CONSTRAINT service_pk
		PRIMARY KEY (service_id),
	CONSTRAINT service_runs_on_route_fk
		FOREIGN KEY (route_id)
		REFERENCES nextbus.route (route_id)
);
-- The Python script forces services with NULL `tag`s.
-- So, use COALESCE to ensure that no duplicates are entered.
CREATE UNIQUE INDEX service_defined_by_route_and_tag_idx
	ON nextbus.service (route_id, COALESCE(tag, ''));

/*
Create stop table.
The PK `stop_id` is created in a Python script.
*/
-- A `route_id`, `tag`, and `location` uniquely define a stop.
CREATE TABLE nextbus.stop (
	stop_id  UUID,
	route_id UUID,
	tag      TEXT,
	name     TEXT,
	location GEOMETRY(POINT, 4326),
	CONSTRAINT stop_pk
		PRIMARY KEY (stop_id),
	CONSTRAINT stop_lies_on_route_fk
		FOREIGN KEY (route_id)
		REFERENCES nextbus.route (route_id)
);
-- Due to error-handling, the Python script might force a stop
--   with a NULL `location`.
-- So, use COALESCE to ensure that no duplicates are entered.
CREATE UNIQUE INDEX stop_defined_by_route_tag_location_idx
	ON nextbus.stop (route_id, tag, COALESCE(TEXT(location), ''));

/*
Create service_stop_order table.
This table shows the order in which stops lie on a route-service.
In case this changes day-to-day, a timestamp is saved on every update.
*/
-- A stop's order on a route-service must be positive.
-- For a given update, a service can't have 2 stops with the same order.
CREATE TABLE nextbus.service_stop_order (
	service_id       UUID,
	stop_id          UUID,
	stop_order       INTEGER,
	update_timestamp TIMESTAMP,
	CONSTRAINT service_stop_match_links_service_fk
		FOREIGN KEY (service_id)
		REFERENCES nextbus.service (service_id),
	CONSTRAINT service_stop_match_links_stop_fk
		FOREIGN KEY (stop_id)
		REFERENCES nextbus.stop (stop_id),
	CONSTRAINT stop_order_is_positive_chk
		CHECK (stop_order > 0),
	CONSTRAINT service_has_valid_stop_order_unq
		UNIQUE (service_id, stop_order, update_timestamp)
);

/*
Create vehicle_location table.
This table shows every updated vehicle GPS location from nextbus.
It also records other vehicle information provided by nextbus.
*/
-- The vehicle_direction must be a valid "degree" angle between 0-360.
-- The vehicle_speed must be nonnegative.
CREATE TABLE nextbus.vehicle_location (
	service_id         UUID,
	vehicle_tag        TEXT,
	vehicle_location   GEOMETRY(POINT, 4326),
	vehicle_direction  NUMERIC,
	vehicle_speed      NUMERIC,
	location_timestamp TIMESTAMP,
	is_predictable     BOOLEAN,
	CONSTRAINT vehicle_provides_service_fk
		FOREIGN KEY (service_id)
		REFERENCES nextbus.service (service_id),
	CONSTRAINT vehicle_direction_is_angle_chk
		CHECK (vehicle_direction  BETWEEN 0 AND 360),
	CONSTRAINT vehicle_speed_is_nonnegative_chk
		CHECK (vehicle_speed >= 0)
);
