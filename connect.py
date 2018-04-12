import urllib
import psycopg2
import psycopg2.extras

# Connect to a postgres database. Tweak some things.
def pgconnect(pghost, pgdb, pguser):
    connection = psycopg2.connect(host = pghost, dbname = pgdb, user = pguser)
    # Set autocommit to avoid repetitive connection.commit() statements.
    connection.autocommit = True
    # Register the UUID adapter globally.
    psycopg2.extras.register_uuid()
    return connection
