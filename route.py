from pyquery import PyQuery as pq
import uuid

# Get a route's current services from the "routeConfig" API endpoint.
# Return them as a list of tuples to be upserted to the database.
def get_services(route):
	route_id  = route[0]
	agency_id = route[1]
	route_tag = route[2]
	# Hit the routeConfig endpoint.
	route_config_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a={0}&r={1}&verbose=true'.format(agency_id, route_tag)
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
