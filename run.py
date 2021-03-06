"""Run through the entire nextbus2pg pipeline:
  1. Connect to the DB.
  2. Update the nextbus agency list.
  3. For the specific agency passed as a sysarg:
    a. Update the routes.
    b. Update the services (called "direction"s by nextbus).
    c. Update the stops.
    d. Update the order in which stops lie on route-services.
    e. Update the most recent vehicle locations for each route.

3e will loop indefinitely with a rest (passed as sysarg) between 
iterations.

3a-d will repeat at the beginning of every day in the timezone passed
as a sysarg.
"""

# PLEASE TAKE NOTE
#
# Since this script loops infinitely, it's up to the user to kill the
# process. PLEASE REMEMBER TO KILL THE SCRIPT before you exhaust your
# storage or rack up crazy $$$ on a DB instance.

import sys
import pytz
import datetime
from time import sleep

import connect
import agency


# Create a dict from the sys args.
# Credit goes entirely to https://gist.github.com/dideler/2395703.
def getopts(argv):
    # Empty dictionary to store key-value pairs.
    opts = {}
    # While there are arguments left to parse...
    while argv:
        # Found a "-name value" pair.
        if argv[0][0] == '-':
            # Add key and value to the dictionary.
            opts[argv[0]] = argv[1]
        # Reduce the argument list by copying it starting from index 1.
        argv = argv[1:]
    return opts


# Process the sysargs.
sysargs = getopts(sys.argv)
# Extract the individual opts from the sysargs.
host      = sysargs['-h']
db        = sysargs['-d']
user      = sysargs['-U']
agency_id = sysargs['-a']
tzone     = sysargs['-z']
resttime  = sysargs['-r']

# Pass the 'timezone' string to pytz.timezone().
user_tz = pytz.timezone(tzone)
# Convert 'resttime' to a float.
resttime = float(resttime)


# Update an agency's routes, services, stops, and service-stop orders.
# Allow to try a number of times, since sometimes some route's services
#   or stops are not added on the first try.
#   TODO: this is a temporary messy workaround.
def update_agency_info(conn, agency_id, n_tries, current_try = 1):
    if current_try <= n_tries:
        try:
            agency.update_routes(conn, agency_id)
            agency.update_services(conn, agency_id)
            agency.update_stops(conn, agency_id)
            agency.update_service_stop_orders(conn, agency_id)
        except:
            update_agency_info(conn, agency_id, n_tries, current_try + 1)

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
#   This will be updated every time the "vehicleLocations" endpoint is
#     hit.
request_times = dict()
# Begin the infinite loop.
while True:
    # Update the agency's info. Try up to 10 times before throwing an
    #   error.
    update_agency_info(conn, agency_id, n_tries = 10)
    # Record the date in the timezone passed as a sysarg.
    utc_now = datetime.datetime.utcnow().replace(tzinfo = pytz.utc)
    latest_route_update = utc_now.astimezone(user_tz).date()
    # Until midnight, keep updating the agency's vehicle locations.
    latest_vehicle_update = latest_route_update
    while latest_vehicle_update == latest_route_update:
        # Record the date.
        #   If midnight has passed, go update the other agency info.
        utc_now = datetime.datetime.utcnow().replace(tzinfo = pytz.utc)
        latest_vehicle_update = utc_now.astimezone(user_tz).date()
        # Rest before continuing.
        sleep(resttime)
        # If vehicle update fails, wait and try again.
        #   This is to catch potential API downtime.
        try:
            request_times = agency.update_vehicle_locations(
                conn, agency_id, request_times
            )
        except:
            continue
