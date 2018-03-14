/*
Set search_path so that postGIS commands are recognized.
*/
SET search_path = postgis, nextbus;

/*
Create agency table.
*/
CREATE TABLE nextbus.agency (
	agency_id TEXT,
	name      TEXT,
	region    TEXT,
	CONSTRAINT agency_pk
		PRIMARY KEY (agency_id)
);
