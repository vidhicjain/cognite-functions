""" handler.py
    Handler to populate time series Site:3W:TPT-64-7434_temperature daily

   isort:skip_file
"""

from io import BytesIO
import pandas as pd
from cognite.client.exceptions import CogniteAPIError, CogniteException
from datetime import datetime, timedelta
from create_event_timeseries import populate_timeseries


def handle(data, client):

    print(f"Hello from {__name__}!")
    print("I got the following data:")
    print(data)

    try:
        timeseries = data.get("timeseries")
        print(data.get("asset_time_series_id"))
        ts_external_id = data.get("asset_time_series_externalId")
        ts_data = client.time_series.retrieve(external_id=ts_external_id).to_pandas()
        print(ts_data)
        ts_internal_id = int(ts_data.loc["id"])
        ts_from = data.get("ts_from")
        ts_to = data.get("ts_to")

        if ts_from == '' or ts_to== '':
            ts_from=str(datetime.today().strftime('%Y-%m-%d'))
            ts_to = str(datetime.today().strftime('%Y-%m-%d'))
            ts_from = datetime.strptime(ts_from, '%Y-%m-%d').date()
            ts_to = datetime.strptime(ts_to, '%Y-%m-%d').date()
        else:
            ts_from = datetime.strptime(ts_from, '%m-%d-%Y').date()
            ts_to = datetime.strptime(ts_to, '%m-%d-%Y').date()
        
        ts_start_date = int(pd.Timestamp(ts_from).timestamp()*1000)
        ts_end_date = pd.Timestamp(ts_to)
        ts_end_date = ts_end_date.replace(hour=23,minute=59,second=59)
        ts_end_date = int(pd.Timestamp(ts_end_date).timestamp()*1000)

        # first delete the ts in 3W in between start and end dates
        ts_to_delete = client.time_series.data.retrieve(external_id=[ts_external_id],limit=None,start=ts_start_date,end=ts_end_date).to_pandas()
        print(ts_to_delete)
        if not ts_to_delete.empty:
            client.time_series.data.delete_range(start=ts_start_date, end=ts_end_date, external_id=ts_external_id)

        date_list = []

        while ts_from <= ts_to:
            date_list.append(ts_from)
            ts_from += timedelta(days=1)

        for ts in timeseries:

            ts_data= ts["timeseries_data"]
            for days in date_list:

                ts_to_simulate_for_day_of_a_week = [
                    day for day in ts_data if day["id"] == days.isoweekday()]
                print(f'event : {ts_to_simulate_for_day_of_a_week} day : {days}')

                # populate_timeseries(
                #     ts_internal_id=ts_internal_id,
                #     ts_external_id=ts_external_id,
                #     file_external_id= ts_to_simulate_for_day_of_a_week[0]["file_external_id"], 
                #     c= client,
                #     event_date= days
                # )

        print(
            f'Successfully inserted data points to the time series : {data.get("asset_time_series_external_name")} for asset : {data.get("asset_external_name")}'
        )

    except (CogniteAPIError, ValueError, CogniteException) as error:

        print(f"Error occured {error}")

    return data
