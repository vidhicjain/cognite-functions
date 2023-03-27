""" handler.py
    Handler to populate time series Site:3W:TPT-64-7434_temperature daily

   isort:skip_file
"""

from cognite.client.exceptions import CogniteAPIError, CogniteException
from datetime import datetime
from create_events import create_events_for_asset


def handle(data, client):

    try:
        asset_names = data.get("assets")
        data_set_id = int(data.get("data_set_id"))
        print(f"data_set_id {data_set_id}")

        for asset in asset_names:

            event_data = asset["event_data"]
            event_to_simulate_for_day_of_a_week = [
                day for day in event_data if day["id"] == datetime.now().isoweekday()
            ]
            print(f'asset : {asset["asset_name"]} event : {event_to_simulate_for_day_of_a_week}')

            create_events_for_asset(
                data_set_id=data_set_id,
                asset_name=str(asset["asset_name"]),
                file_internal_id=int(event_to_simulate_for_day_of_a_week[0]["file_id"]),
                event_type=int(event_to_simulate_for_day_of_a_week[0]["event_type"]),
                c=client,
            )

    except (CogniteAPIError, ValueError, CogniteException) as error:

        print(f"Error occured {error}")

    return event_to_simulate_for_day_of_a_week
