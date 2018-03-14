/*
Set search_path so that postGIS commands are recognized.
*/
SET search_path = postgis, nextbus;

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
