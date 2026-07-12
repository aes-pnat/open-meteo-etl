from dataclasses import dataclass


@dataclass(frozen=True)
class Location:
    city: str
    latitude: float
    longitude: float
    timezone: str  # IANA timezone string — required by Open-Meteo


LOCATIONS: list[Location] = [
    Location("Atlanta",        33.7490,   -84.3880,  "America/New_York"),
    Location("Charlotte",      35.2271,   -80.8431,  "America/New_York"),
    Location("Chicago",        41.8781,   -87.6298,  "America/Chicago"),
    Location("Cleveland",      41.4993,   -81.6944,  "America/New_York"),
    Location("Copenhagen",     55.6761,    12.5683,  "Europe/Copenhagen"),
    Location("Denver",         39.7392,  -104.9903,  "America/Denver"),
    Location("Las Vegas",      36.1699,  -115.1398,  "America/Los_Angeles"),
    Location("Minneapolis",    44.9778,   -93.2650,  "America/Chicago"),
    Location("New York",       40.7128,   -74.0060,  "America/New_York"),
    Location("Philadelphia",   39.9526,   -75.1652,  "America/New_York"),
    Location("Pittsburgh",     40.4406,   -79.9959,  "America/New_York"),
    Location("Scottsdale",     33.4942,  -111.9261,  "America/Phoenix"),
    Location("Seattle",        47.6062,  -122.3321,  "America/Los_Angeles"),
    Location("St Louis",       38.6270,   -90.1994,  "America/Chicago"),
    Location("Manchester",     53.4808,    -2.2426,  "Europe/London"),
    Location("Boston",         42.3601,   -71.0589,  "America/New_York"),
    Location("San Diego",      32.7157,  -117.1611,  "America/Los_Angeles"),
    Location("Salt Lake City", 40.7608,  -111.8910,  "America/Denver"),
    Location("New Orleans",    29.9511,   -90.0715,  "America/Chicago"),
    Location("Santa Monica",   34.0195,  -118.4912,  "America/Los_Angeles"),
    Location("Detroit",        42.3314,   -83.0458,  "America/Detroit"),
    Location("Sydney",        -33.8688,   151.2093,  "Australia/Sydney"),
    Location("London",         51.5074,    -0.1278,  "Europe/London"),
]
