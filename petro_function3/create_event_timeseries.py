from io import BytesIO
import pandas as pd
from cognite.client.exceptions import CogniteAPIError, CogniteException
from datetime import date, timedelta
from cognite.client import CogniteClient

def populate_timeseries(
        ts_internal_id:int,ts_external_id:str,file_external_id: int, c: CogniteClient,event_date: date
        ):
    try: 
        print(f"Hello from {__name__}!")
        print("I got the following data:")
        ts_internal_id = ts_internal_id
        print(ts_internal_id)
        ts_external_id = ts_external_id
        file_external_id = file_external_id
        c = c
        event_date = event_date
        file_content = c.files.download_bytes(external_id=file_external_id)

        print(f"downloaded file :{type(file_content)}")

        df = pd.read_csv(BytesIO(file_content), index_col=0, parse_dates=True)
        print(f"DF1 :{df.info()}")
        df_t_tpt = df[["P-TPT"]]
        # Add Timeseries Id to the DF
        print(ts_internal_id)
        df_t_tpt.columns = [ts_internal_id]
        df_to_load = df_t_tpt
        print(f"DF1 :{df_t_tpt.info()}")
        df_to_load.reset_index(inplace=True)

        today = event_date
        yesterday = today - timedelta(days=1)
        df_to_load["Date"] = df_to_load["timestamp"].dt.date
        df_to_load["Time"] = df_to_load["timestamp"].dt.time

        unique_values = list(df_to_load["Date"].unique())
        unique_values.extend([yesterday, today])

        print(f"length of list : {len(unique_values)}")
        if len(unique_values) == 3:
            
            df_to_load.loc[df_to_load["Date"] == unique_values[0], "Date"] = unique_values[2]

        elif len(unique_values) == 4:
           
            df_to_load.loc[df_to_load["Date"] == unique_values[0], "Date"] = unique_values[3]
            df_to_load.loc[df_to_load["Date"] == unique_values[1], "Date"] = unique_values[2]

        df_to_load["timestamp"] = pd.to_datetime(df_to_load["Date"].astype(str) + " " + df_to_load["Time"].astype(str))
        print(df_to_load)
        df_to_load = df_to_load.set_index("timestamp")
        df_to_load.drop(["Date", "Time"], inplace=True, axis=1)
        print(f"DF :{df_to_load.head(n=5)}")
        c.datapoints.insert_dataframe(df_to_load, dropna=True, external_id_headers=False)

        print(
            f'Successfully inserted data points to the time series : {ts_external_id} for asset : {ts_internal_id}'
        )

    except (CogniteAPIError, ValueError, CogniteException) as error:

        print(f"Error occured {error}")
