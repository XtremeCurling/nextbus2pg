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
