from pyquery import PyQuery as pq
import psycopg2
import psycopg2.extras
import uuid

# Get the current "agencyList" from the nextbus API. Upsert to the postgres database.
def update_agency(conn):
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
def update_route(conn, agency_id):
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
