
def _raw_dbapi_connection(dbapi_conn):
    # Utility to unwrap any pool proxy
    connection = getattr(dbapi_conn, 'dbapi_connection', dbapi_conn)
    return connection
