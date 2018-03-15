from pyquery import PyQuery as pq
import uuid
import psycopg2
import datetime

# Get a route's current services from the "routeConfig" API endpoint.
# Return them as a list of tuples to be upserted to the database.
def get_services(route):
	route_id  = route[0]
	agency_id = route[1]
	route_tag = route[2]
	# Hit the routeConfig endpoint.
	route_config_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a={0}&r={1}&verbose=true'.format(
			agency_id, route_tag
		)
	)
	# Format the route's services as a list of tuples for psycopg2.
	service_rows = [(
		uuid.uuid4(),
		route_id,
		i.attr('tag'),
		i.attr('title'),
		i.attr('name'),
		i.attr('useforui') == 'true'
	) for i in route_config_pq.items('direction')]
	# Include a NULL service tag, used for vehicles that are not currently running a service.
	service_rows.extend([(uuid.uuid4(), route_id, None, None, None, False)])
	# Return the list of service tuples.
	return service_rows

# Get a route's current stops from the "routeConfig" API endpoint.
#   Also note which stops show up under the 'direction' headings, but not in the body of the XML.
#   These stops are considered "missing", and are dealt with in a subsequent step.
# Return both lists of tuples: the stops in the body of the routeConfig XML, and the "missing" stops.
def get_stops(route):
	route_id  = route[0]
	agency_id = route[1]
	route_tag = route[2]
	# Hit the routeConfig endpoint.
	route_config_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a={0}&r={1}&verbose=true'.format(
			agency_id, route_tag
		)
	)
	# Format the route's stops as a list of tuples for psycopg2.
	# These will be passed to the mogrify function so that postgis commands can be wrapped around them.
	stop_rows = [(
		uuid.uuid4(),
		route_id,
		i.attr('tag'),
		i.attr('title'),
		i.attr('lon'),
		i.attr('lat')
	) for i in route_config_pq.items('body > route > stop')]
	# Record the "missing stops": those that show up somewhere in the XML, but not in the body.
	# Store them as a set to avoid duplicates.
	all_stops     = set(i.attr('tag') for i in route_config_pq.items('stop'))
	missing_stops = set((route_id, s) for s in all_stops \
		if s not in [sa[2] for sa in stop_rows])
	# Return a list with (1) the stop_rows list and (2) the missing_stops set
	return [stop_rows, missing_stops]

# Get a route's current service stop orders from the "routeConfig" API endpoint.
# Return them as a list of tuples to be upserted to the database.
def get_service_stop_orders(conn, route):
	route_id  = route[0]
	agency_id = route[1]
	route_tag = route[2]
	# Get the current UTC datetime.
	now = datetime.datetime.utcnow()
	# Hit the routeConfig endpoint.
	route_config_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a={0}&r={1}&verbose=true'.format(
			agency_id, route_tag
		)
	)
	# Get all services running on and stops lying on the current route.
	with conn.cursor() as cur:
		# Get services.
		cur.execute("SELECT tag, service_id FROM nextbus.service WHERE route_id = %s", (route_id,))
		services = cur.fetchall()
		# Get stops.
		cur.execute("SELECT tag, stop_id FROM nextbus.stop WHERE route_id = %s", (route_id,))
		stops = cur.fetchall()
	# Create dicts from (key) tag -> (value) UUID for the route's services and stops.
	service_dict = dict(services)
	stop_dict    = dict(stops)
	# Initiate a list to store the direction stop orders on the "routeConfig" endpoint results.
	stop_orders = []
	# Add the service stop orders for each service (or "direction" on the endpoint results).
	for i in route_config_pq.items('direction'):
		stop_order = 1
		for j in i.items('stop'):
			stop_orders.extend([(i.attr('tag'), j.attr('tag'), stop_order)])
			stop_order += 1
	# Use the dicts to find the service and stop UUIDs, and return the resulting list of tuples.
	stop_order_rows = [(
		service_dict[so[0]],
		stop_dict[so[1]],
		so[2],
		now
	) for so in stop_orders]
	return stop_order_rows

# Get a route's current vehicle locations from the "vehicleLocations" API endpoint.
# Return as a list of tuples to be passed to the mogrify function before being inserted to the database.
def get_vehicle_locations(conn, route, service_dict, route_service_dict, previous_request):
	route_id  = route[0]
	agency_id = route[1]
	route_tag = route[2]
	# Hit the vehicleLocations endpoint.
	vehicle_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=vehicleLocations&a={0}&r={1}&t={2}'.format(
			agency_id, route_tag, previous_request
		)
	)
	# Get the time (in epoch microseconds since 1970) of this API request.
	# This will be returned along with the vehicle locations.
	this_request = vehicle_pq('lastTime').attr('time')
	# Convert to a UTC datetime representation. This will be used to populate the location_datetime field.
	request_datetime = datetime.datetime.utcfromtimestamp(round(float(this_request) / 1000))
	# Initiate the list of tuples that will contain the route's vehicle locations.
	vehicle_rows = []
	# Loop through each vehicle to create its tuple of column values for postgres.
	for i in vehicle_pq.items('vehicle'):
		# Match 'dirTag's to service UUIDs as follows:
		#   (1) Try to find 'dirTag' in the route_service_dict.
		#   (2) If (1) doesn't work, try to find 'dirTag' in the agency-wide service_dict.
		#   (3) If (2) doesn't work, skip to the next vehicle in the for loop.
		try:
			service_id = route_service_dict[i.attr('dirtag')]
		except:
			try:
				service_id = service_dict[i.attr('dirtag')]
			except:
				print(i.attr('dirtag') + " is not a valid service tag for agency " + agency_id)
				continue
		# If the speed is not a valid float, set to NULL.
		try:
			vehicle_speed = float(i.attr('speedkmhr'))
		except:
			vehicle_speed = None
		# If the direction is not a valid float, set to NULL.
		try:
			vehicle_direction = float(i.attr('heading'))
		except:
			vehicle_direction = None
		# Extend the vehicle_rows list to include a tuple containing this vehicle's most recent
		# location and other information.
		vehicle_rows.extend([(
			service_id,
			i.attr('id'),
			i.attr('lon'),
			i.attr('lat'),
			vehicle_direction if 0 <= vehicle_direction <= 360 else None,
			vehicle_speed if vehicle_speed >= 0 else None,
			request_datetime - datetime.timedelta(seconds = float(i.attr('secssincereport'))),
			i.attr('predictable') == 'true'
		)])
	# Return the vehicle rows, and the epoch time of the API request.
	return [vehicle_rows, this_request]
