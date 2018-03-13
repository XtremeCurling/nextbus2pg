from pyquery import PyQuery as pq
import psycopg2
import psycopg2.extras
import uuid
import route

# Get the current "agencyList" from the nextbus API. Upsert to the postgres database.
def update_agencies(conn):
	# Hit the agencyList endpoint.
	agency_pq = pq(url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=agencyList')
	# Format the results as a list of tuples for psycopg2.
	agency_rows = [(
		i.attr('tag'),
		i.attr('title'),
		i.attr('regiontitle')
	) for i in agency_pq.items('agency')]
	# Create the UPSERT command.
	# If agency is already in database, update its name and region.
	upsert_sql = '''
		INSERT INTO nextbus.agency (agency_id, name, region)
			VALUES %s
			ON CONFLICT (agency_id)
			DO UPDATE SET
				(name, region) = (EXCLUDED.name, EXCLUDED.region)
	'''
	# Execute the UPSERT command.
	with cur as conn.cursor():
		psycopg2.extras.execute_values(
			cur, upsert_sql, agency_rows
		)

# Get an agency's current "routeList" from the nextbus API. Upsert to the postgres database.
def update_routes(conn, agency_id):
	# Hit the routeList endpoint.
	route_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeList&a={0}'.format(agency_id)
	)
	# Format the results as a list of tuples for psycopg2.
	route_rows = [(
		uuid.uuid4(),
		agency_id,
		i.attr('tag'),
		i.attr('title')
	) for i in route_pq.items('route')]
	# Create the UPSERT command.
	# If route is already in database, update its name.
	upsert_sql = '''
		INSERT INTO nextbus.route (route_id, agency_id, tag, name)
			VALUES %s
			ON CONFLICT (agency_id, tag)
			DO UPDATE SET
				(name) = (EXCLUDED.name)
	'''
	# Execute the UPSERT command.
	with cur as conn.cursor()
		psycopg2.extras.execute_values(
			cur, upsert_sql, route_rows
		)

# Get an agency's current route "services", found in each route's "routeConfig" from the nextbus API.
# Upsert to the postgres database.
def update_services(conn, agency_id):
	# Get all of the agency's routes with their UUIDs.
	with cur as conn.cursor():
		cur.execute("SELECT * FROM nextbus.route WHERE agency_id = %s", (agency_id,))
		routes = cur.fetchall()
	# Initiate the list that will contain all of the service rows.
	service_rows = []
	# For each route, hit the routeConfig API endpoint and get all the service info contained within.
	for r in routes:
		service_rows.extend(route.get_services(route = r))
	# Create the UPSERT command.
	# If service is already in database, update its name, direction, and use_for_ui boolean.
	upsert_sql = '''
		INSERT INTO nextbus.service (service_id, route_id, tag, name, direction, use_for_ui)
			VALUES %s
			ON CONFLICT (route_id, COALESCE(tag, ''))
			DO UPDATE SET
				(name, direction, use_for_ui)
				= (EXCLUDED.name, EXCLUDED.direction, EXCLUDED.use_for_ui)
	'''
	# Execute the UPSERT command.
	with cur as conn.cursor():
		psycopg2.extras.execute_values(
			cur, upsert_sql, service_rows
		)

# Get an agency's current stops, found in each route's "routeConfig" from the nextbus API.
# Upsert to the postgres database.
def update_stops(conn, agency_id):
	# Get all of the agency's routes with their UUIDs.
	with cur as conn.cursor():
		cur.execute("SELECT * FROM nextbus.route WHERE agency_id = %s", (agency_id,))
		routes = cur.fetchall()
	# Initiate the list that will contain all of the stop tuples.
	# These will be passed to the mogrify function so that postgis commands can be wrapped around them.
	stop_rows = []
	# Initiate the set that will contain all missing stops.
	missing_stops = set()
	# For each route, hit the routeConfig API endpoint and get all the stop info contained within.
	for r in routes:
		[r_stop_rows, r_missing_stops] = route.get_stops(route = r)
		stop_rows.extend(r_stop_rows)
		missing_stops.update(r_missing_stops)
	# For each missing stop, first see if any other stops exist with the same tag under a different
	# route; if so, use that stop's name and location; and if not, create a stop row with NULL name
	# and location.
	for ms in missing_stops:
		matching_stop_rows = [sr for sr in stop_rows if sr[2] == ms[1]]
		# If at least one existing stop matches the missing stop's tag, use the name and lon/lat
		# from one of these matching stops.
		if len(matching_stop_rows) > 0:
			# Sort so that choice of stop is deterministic.
			# Sort by (tag, lon, lat, name, route_id).
			matching_stop_rows.sort(key = lambda sr: (sr[2], sr[4], sr[5], sr[3], sr[1]))
			matching_stop = matching_stop_rows[0]
			new_stop_row  = [(
				uuid.uuid4(),
				ms[0],
				ms[1],
				matching_stop[3],
				matching_stop[4],
				matching_stop[5]
			)]
		# If no existing stop matches the missing stop's tag, set NULL name and lon/lat.
		else:
			new_stop_row = [(
				uuid.uuid4(),
				ms[0],
				ms[1],
				None,
				None,
				None
			)]
		stop_rows.extend(new_stop_row)
	# Wrap postgis command around the lon and lat of each stop.
	stop_rows_str = ','.join(cur.mogrify("(%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))", i) \
		for i in stop_rows)
	# Execute an UPSERT command.
	# If stop with same route, tag, and location is already in database, update its name.
	with cur as conn.cursor():
		cur.execute(
			"INSERT INTO nextbus.stop (stop_id, route_id, tag, name, location) " \
			+ "SELECT DISTINCT ON (route_id, tag, location) * " \
			+ "FROM (VALUES " + stop_rows_str + ") v(stop_id, route_id, tag, name, location) " \
			+ "ON CONFLICT (route_id, tag, COALESCE(location, '')) " \
			+ "DO UPDATE SET (name) = (EXCLUDED.name)"
		)

# Get an agency's current service stop orders, found in each route's "routeConfig" from the nextbus API.
# Upsert to the postgres database.
def get_service_stop_orders(conn, agency_id):
	# Get all of the agency's routes and services with their UUIDs.
	with cur as conn.cursor():
		cur.execute("SELECT * FROM nextbus.route WHERE agency_id = %s", (agency_id,))
		routes = cur.fetchall()
		cur.execute(
			"SELECT service_id, route_id, service.tag, service.name, direction, use_for_ui " \
			+ "FROM nextbus.service INNER JOIN nextbus.route USING (route_id) " \
			+ "WHERE agency_id = %s", (agency_id,)
		)
		services = cur.fetchall()
	# Initiate the list that will contain all of the service stop order rows.
	order_rows = []
	# For each route, find the order of stops for each service.
	for r in routes:
		order_rows.extend(route.get_service_stop_orders(conn = conn, route = r))
	# Create the UPSERT command.
	upsert_sql = '''
		INSERT INTO nextbus.service_stop_order (service_id, stop_id, stop_order, update_timestamp)
			VALUES %s
			ON CONFLICT (service_id, stop_order, update_timestamp)
			DO NOTHING
	'''
	# Execute the UPSERT command.
	with cur as conn.cursor():
		psycopg2.extras.execute_values(
			cur, upsert_sql, order_rows
		)
