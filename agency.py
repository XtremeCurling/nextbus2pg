from pyquery import PyQuery as pq
import psycopg2
import psycopg2.extras

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
