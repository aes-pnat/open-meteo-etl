# TODO: Unit tests for src/extraction/api_client.py
# Use unittest.mock to avoid real HTTP calls.
#
# Suggested cases:
#   - 200 response returns raw JSON unchanged
#   - Non-200 status raises a descriptive exception
#   - HTTP 200 with error body also raises
#   - Failed location is skipped, not raised, in fetch_all_locations
