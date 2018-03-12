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

# Get a route's current stops from the "routeConfig" API endpoint.
#   Also note which stops show up under the 'direction' headings, but not in the body of the XML.
#   These stops are considered "missing", and are saved for a later step in the logic.
# Return both lists of tuples: the stops in the body of the routeConfig XML, and the "missing" stops.
def get_stops(route):
	route_id  = route[0]
	agency_id = route[1]
	route_tag = route[2]
	# Hit the routeConfig endpoint.
	route_config_pq = pq(
		url = 'http://webservices.nextbus.com/service/publicXMLFeed?command=routeConfig&a={0}&r={1}&verbose=true'.format(agency_id, route_tag)
	)
	# Format the route's stops as a list of tuples for psycopg2.
	# These will be passed to the mogrify function so that postgis commands can be wrapped around them.
	stop_args = [(
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
		if s not in [sa[2] for sa in stop_args])
	# Return a list with (1) the stop_args list and (2) the missing_stops set
	return [stop_args, missing_stops]
