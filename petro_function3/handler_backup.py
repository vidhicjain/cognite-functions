""" handler.py
    Handler to populate time series Site:3W:TPT-64-7434_temperature daily

   isort:skip_file
"""

from io import BytesIO
import pandas as pd
from cognite.client.exceptions import CogniteAPIError, CogniteException
from datetime import date, timedelta


def handle(data, client):

    print(f"Hello from {__name__}!")
    print("I got the following data:")
    print(data)

    try:
        file_internal_id = data.get("file_id", 7083372046210856)
        ts_internal_id = data.get("asset_time_series_id")
        print(f"ts_internal_id :{ts_internal_id}")
        file_content = client.files.download_bytes(id=file_internal_id)

        print(f"downloaded file :{type(file_content)}")

        df = pd.read_csv(BytesIO(file_content), index_col=0, parse_dates=True)
        print(f"DF1 :{df.head(n=5)}")
        df_t_tpt = df[["P-TPT"]]
        # Add Timeseries Id to the DF

        df_t_tpt.columns = [ts_internal_id]
        df_to_load = df_t_tpt
        df_to_load.reset_index(inplace=True)

        # Split the timestamp into date and time and get unique values of date to replace with current date time
        today = date.today()
        yesterday = today - timedelta(days=1)
        df_to_load["Date"] = df_to_load["timestamp"].dt.date
        df_to_load["Time"] = df_to_load["timestamp"].dt.time

        unique_values = list(df_to_load["Date"].unique())
        unique_values.extend([yesterday, today])

        df_to_load.loc[df_to_load["Date"] == unique_values[0], "Date"] = unique_values[2]
        df_to_load.loc[df_to_load["Date"] == unique_values[1], "Date"] = unique_values[3]

        df_to_load["timestamp"] = pd.to_datetime(df_to_load["Date"].astype(str) + " " + df_to_load["Time"].astype(str))
        df_to_load = df_to_load.set_index("timestamp")
        df_to_load.drop(["Date", "Time"], inplace=True, axis=1)
        print(f"DF :{df_to_load.head(n=5)}")
        client.datapoints.insert_dataframe(df_to_load, dropna=True, external_id_headers=False)

        print(
            f'Successfully inserted data points to the time series : {data.get("asset_time_series_external_name")} for asset : {data.get("asset_external_name")}'
        )

    except (CogniteAPIError, ValueError, CogniteException) as error:

        print(f"Error occured {error}")

    return data
