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
	# Wrap postgis command around the lon and lat of each stop.
	stop_rows_str = ','.join(cur.mogrify("(%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))", i) \
		for i in stop_rows)
	# Execute an UPSERT command.
	# If stop with same location is already in database, update its name.
	with cur as conn.cursor():
		cur.execute(
			"INSERT INTO nextbus.stop (stop_id, route_id, tag, name, location) " \
			+ "SELECT DISTINCT ON (route_id, tag, location) * " \
			+ "FROM (VALUES " + stop_rows_str + ") v(stop_id, route_id, tag, name, location) " \
			+ "ON CONFLICT (route_id, tag, location) " \
			+ "DO UPDATE SET (name) = (EXCLUDED.name)"
		)
	# Return the set of missing stop tuples.
	return missing_stops
