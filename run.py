import sys
import pytz
import datetime
from time import sleep
import nextbus2pg.connect as connect
import nextbus2pg.agency as agency

### This script runs through the entire nextbus2pg pipeline:
###   1. Connect to the DB.
###   2. Update the nextbus agency list.
###   3. For the specific agency passed as a sysarg:
###     a. Update the routes.
###     b. Update the services (called "direction"s by nextbus).
###     c. Update the stops.
###     d. Update the order in which stops lie on route-services.
###     e. Update the most recent vehicle locations for each route.
### 3e will loop indefinitely with a rest (passed as sysarg) between iterations.
### 3a-d will repeat at the beginning of every day in the timezone passed as a sysarg.

# PLEASE TAKE NOTE
# Since this script loops infinitely, it's up to the user to kill the process.
# PLEASE REMEMBER TO KILL THE SCRIPT
#   - before you exhaust your storage or rack up crazy $$$ on a DB instance.

# Create a dict from the sys args.
# Credit goes entirely to user RobinCheptileh's comment at
#    https://gist.github.com/dideler/2395703.
def getopts(argv):
    opts = {}
    while argv:
        if argv[0][0] == '-':
            if argv[0] in opts:
                opts[argv[0]].append(argv[1])
            else:
                opts[argv[0]] = [argv[1]]
        argv = argv[1:] 
    return opts

# Process the sysargs.
sysargs = getopts(sys.argv)
# Extract the individual opts from the sysargs.
host      = sysargs['h']
db        = sysargs['d']
user      = sysargs['U']
agency_id = sysargs['a']
tzone     = sysargs['z']
resttime  = sysargs['r']

# Pass the 'timezone' string to pytz.timezone().
user_tz = pytz.timezone(tzone)
# Convert 'resttime' to a float.
resttime = float(resttime)

# Connect to the PG database.
# host, db, and user should be passed through the sysargs using flags
#   -h, -d, and -U, respectively.
# Make sure you have a ~/.pgpass file that includes this db instance.
#   This is where the port and password will be obtained.
conn = connect.pgconnect(
	pghost = host,
	pgdb   = db,
	pguser = user
)

# Update the nextbus agency list.
agency.update_agencies(conn)
# Set `request_times` to an empty dict.
#   This will be updated every time the "vehicleLocations" endpoint is hit.
request_times = dict()
# Begin the infinite loop.
while True:
	# Update the agency's routes.
	agency.update_routes(conn, agency_id)
	# Update the agency's services.
	agency.update_services(conn, agency_id)
	# Update the agency's stops.
	agency.update_stops(conn, agency_id)
	# Update the agency's servic-stop orders.
	agency.update_service_stop_orders(conn, agency_id)
	# Record the date in the timezone passed as a sysarg.
	utc_now = datetime.datetime.utcnow().replace(tzinfo = pytz.utc)
	latest_route_update = utc_now.astimezone(user_tz).date()
	# Until midnight, keep updating the agency's vehicle locations.
	latest_vehicle_update = latest_route_update
	while latest_vehicle_update == latest_route_update:
		request_times = update_vehicle_locations(
			conn, agency_id, request_times
		)
		# Record the date.
		#   If midnight has passed, go update the other agency info.
		utc_now = datetime.datetime.utcnow().replace(tzinfo = pytz.utc)
		latest_vehicle_update = utc_now.astimezone(user_tz).date()
		# Rest before the next iteration.
		sleep(resttime)
