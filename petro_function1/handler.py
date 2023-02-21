""" handler.py
    Handler to populate time series Site:3W:TPT-64-7434_temperature daily

   isort:skip_file
"""

from io import BytesIO
import pandas as pd
from cognite.client.exceptions import CogniteAPIError, CogniteException


def handle(data, client):

    print(f"Hello from {__name__}!")
    print("I got the following data:")
    print(data)
    # TODO Change the timestamp to current execution  date of this schedule
    try:
        file_internal_id = data.get("file_id", 7083372046210856)
        ts_internal_id = data.get("asset_time_series_id")
        print(f"ts_internal_id :{ts_internal_id}")
        file_content = client.files.download_bytes(id=file_internal_id)
        print(f"downloaded file :{type(file_content)}")

        data = BytesIO(file_content)
        df = pd.read_csv(data)
        df_t_tpt = df[["T-TPT"]]
        # Add Timeseries Id to the DF
        df_t_tpt.columns = ts_internal_id
        print(f"DF :{df_t_tpt.head(n=5)}")
        client.datapoints.insert_dataframe(df_t_tpt, dropna=True, external_id_headers=False)
        print(
            f'Successfully inserted data points to the time series : {data.get("asset_time_series_external_name")} for asset : {data.get("asset_external_name")}'
        )
        return df_t_tpt.shape
    except (CogniteAPIError, ValueError, CogniteException) as error:
        print(f"Error occured {error.message}")
        return error.message
