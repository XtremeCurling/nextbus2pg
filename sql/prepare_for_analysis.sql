/*
Delete duplicate instances of (service_id, vehicle_tag, location_timestamp)
from nextbus.vehicle_location.
*/
-- Credit goes to https://stackoverflow.com/a/46775289
DELETE FROM nextbus.vehicle_location vl1
    USING nextbus.vehicle_location vl2
    WHERE vl1.service_id = vl2.service_id
        AND vl1.vehicle_tag = vl2.vehicle_tag
        AND vl1.location_timestamp = vl2.location_timestamp
        AND vl1.ctid < vl2.ctid; -- Keep only the most recent record

/*
Create indexes on fields to be used in WHERE and GROUP BY clauses.
*/
CREATE INDEX vehicle_location_service_idx
	ON nextbus.vehicle_location (service_id);
CREATE INDEX vehicle_location_tag_idx
	ON nextbus.vehicle_location (vehicle_tag);
CREATE INDEX vehicle_location_location_idx
	ON nextbus.vehicle_location USING GIST (vehicle_location);
CREATE INDEX vehicle_location_timestamp_idx
	ON nextbus.vehicle_location (location_timestamp);
CREATE INDEX vehicle_location_predictable_idx
	ON nextbus.vehicle_location (is_predictable);
