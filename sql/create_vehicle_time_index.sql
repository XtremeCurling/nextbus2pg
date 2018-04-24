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
