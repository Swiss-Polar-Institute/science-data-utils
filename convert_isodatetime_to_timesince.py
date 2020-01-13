import datetime

def convert_isodatetime_to_timesince_seconds(initial, isodatetime):
    """Convert a datetime in ISO8601 format YYYY-MM-DDTHH:MI:SS+00:00 where +00:00 is the timezone and needs to be
    specified, to a time since a specified initial datetime given in the same format. Return the output in seconds."""

    # Convert the initial and ISO datetimes to Python datetime formats (rather than strings)
    initial_dt = datetime.datetime.strptime(initial, '%Y-%m-%dT%H:%M:%S+00:00')
    isodatetime_dt = datetime.datetime.strptime(isodatetime, '%Y-%m-%d %H:%M:%S+00:00')

    # Calculate the time difference between the two datetimes
    timedelta = isodatetime_dt - initial_dt

    # Convert the time difference to seconds
    timedelta_secs = timedelta.total_seconds()

    return timedelta_secs