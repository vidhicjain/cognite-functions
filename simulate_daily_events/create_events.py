import re

from datetime import date, timedelta
from io import BytesIO

import pandas as pd

from cognite.client import CogniteClient
from cognite.client.data_classes import Event
from cognite.client.exceptions import CogniteAPIError, CogniteException


pd.options.display.max_columns = None
pattern = re.compile(r"^Site:3W:TPT.*$")

EVENT_MAP = {
    7: "SCALING_IN_PCK_FAILURE",
    1: "ABRUPT_INCREASE_OF_BSW_FAILURE",
    107: "SCALING_IN_PCK_TRANSIT",
    101: "ABRUPT_INCREASE_OF_BSW_TRANSIT",
    0: "ASSET_NORMAL_CONDITION",
    103: "SEVERE_SLUGGING_TRANSIT",
    3: "SEVERE_SLUGGING_FAILURE",
    104: "FLOW_INSTABILITY_TRANSIT",
    4: "FLOW_INSTABILITY_FAILURE",
    105: "RAPID_PRODUCTIVITY_LOSS_TRANSIT",
    5: "RAPID_PRODUCTIVITY_LOSS_FAILURE",
    106: "QUICK_RESTRICTION_IN_PCK_TRANSIT",
    6: "QUICK_RESTRICTION_IN_PCK_FAILURE",
    108: "HYDRATE_IN_PRODUCTION_LINE_TRANSIT",
    8: "HYDRATE_IN_PRODUCTION_LINE_FAILURE",
}


def __get_event_df(event_df: pd.DataFrame, event_type: int) -> pd.DataFrame:

    # The timestamp index in the CSV file is already sorted
    response_df: pd.DataFrame = None

    try:
        df = event_df.loc[(event_df["class"] == event_type)]

        df1 = df.iloc[[0]]

        df1.reset_index(inplace=True)
        df1.columns = ["start_timestamp", "start_value", "class"]
        df2 = df.iloc[[-1]]

        df2.reset_index(inplace=True)
        df2.columns = ["end_timestamp", "end_value", "class"]
        df3 = df1.merge(df2, how="inner")
        response_df = df3
        return response_df
    except (IndexError) as error:
        print(error)


def __populate_event(
    event_df: pd.DataFrame, event_type: int, measure: str, asset_ids: list, unit: list, data_set_id: int
) -> Event:

    # Now populate Events
    start_timestamp = event_df["start_timestamp"].iloc[0]
    end_timestamp = event_df["end_timestamp"].iloc[0]
    v_date1 = pd.Timestamp(start_timestamp).timestamp()
    v_date2 = pd.Timestamp(end_timestamp).timestamp()
    metadata = {}
    if measure != "PDG":
        metadata = {
            "t_start_value": event_df["start_value"].iloc[0][0],
            "t_end_value": event_df["end_value"].iloc[0][0],
            "t_unit": unit[0],
            "p_start_value": event_df["start_value"].iloc[0][1],
            "p_end_value": event_df["end_value"].iloc[0][1],
            "p_unit": unit[1],
        }
    else:
        metadata = {
            "p_start_value": event_df["start_value"].iloc[0],
            "p_end_value": event_df["end_value"].iloc[0],
            "p_unit": unit[0],
        }

    event = Event(
        start_time=v_date1 * 1000,
        end_time=v_date2 * 1000,
        metadata=metadata,
        data_set_id=data_set_id,
        type=EVENT_MAP[event_type],
        description=str(f"Event {EVENT_MAP[event_type]} Occured "),
        subtype=measure,
        source="3W",
        asset_ids=asset_ids,
    )
    print(event)
    return event


def create_events_for_asset(
    asset_name: str, event_type: int, data_set_id: int, file_internal_id: int, c: CogniteClient
):

    try:

        file_content = c.files.download_bytes(id=file_internal_id)

        print(f"downloaded file :{type(file_content)}")

        df = pd.read_csv(BytesIO(file_content), index_col=0, parse_dates=True)
        # Simulate the events for today
        df_to_load = df
        df_to_load.reset_index(inplace=True)

        # Split the timestamp into date and time and get unique values of date to replace with current date time
        today = date.today()
        yesterday = today - timedelta(days=1)
        df_to_load["Date"] = df_to_load["timestamp"].dt.date
        df_to_load["Time"] = df_to_load["timestamp"].dt.time

        unique_values = list(df_to_load["Date"].unique())
        unique_values.extend([today,yesterday])
        print(f"length of list : {len(unique_values)}")
        if len(unique_values) == 3:
            print(f"inside if : {unique_values}")
            df_to_load.loc[df_to_load["Date"] == unique_values[0], "Date"] = unique_values[2]

        elif len(unique_values) == 4:
            print(f"inside else : {unique_values}")
            df_to_load.loc[df_to_load["Date"] == unique_values[0], "Date"] = unique_values[3]
            df_to_load.loc[df_to_load["Date"] == unique_values[1], "Date"] = unique_values[2]

        df_to_load["timestamp"] = pd.to_datetime(df_to_load["Date"].astype(str) + " " + df_to_load["Time"].astype(str))
        df_to_load = df_to_load.set_index("timestamp")
        df_to_load.drop(["Date", "Time"], inplace=True, axis=1)

        print(f"DF Head :{df_to_load.head(n=5)}")
        print(f"DF Tail :{df_to_load.tail(n=5)}")

        event_store = []

        w_assets = (
            c.assets.list(data_set_ids=[data_set_id], limit=1, name=asset_name)
            .to_pandas()[["name", "id"]]
            .set_index("name")["id"]
            .to_dict()
        )
        asset = next(iter(w_assets))
        print(f"The asset name to associate the event : {asset}")
        if pattern.match(str(asset)) is not None:

            df_t_tpt = df_to_load[["T-TPT", "class", "P-TPT"]]
            df_t_tpt["new_col"] = df_t_tpt.apply(lambda x: [x["T-TPT"], x["P-TPT"]], axis=1)

            df_t_tpt = df_t_tpt[["new_col", "class"]]
            # Add Asset Id to the DF
            df_t_tpt.columns = [w_assets[str(asset).split("_")[0]], "class"]

            # Prepare DF for Transit Event
            df_transit = __get_event_df(df_t_tpt, event_type + 100)

            ls_assets = list(df_t_tpt.columns.values)
            ls_assets.remove("class")

            if df_transit is not None:
                event_store.append(
                    __populate_event(df_transit, event_type + 100, "TPT", ls_assets, ["Celsius", "Pa"], data_set_id)
                )

            # Prepare DF for FAILURE EVENT

            df_failure = __get_event_df(df_t_tpt, event_type)
            if df_failure is not None:
                event_store.append(
                    __populate_event(df_failure, event_type, "TPT", ls_assets, ["Celsius", "Pa"], data_set_id)
                )

        else:
            print(f"pressure PDG :{asset}")
            # PDG asset will have only pressure measurement
            df_p_pdg = df_to_load[["P-PDG", "class"]]
            df_p_pdg.columns = [w_assets[str(asset).split("_")[0]], "class"]

            # Prepare DF for Transit Event
            df_transit = __get_event_df(df_p_pdg, event_type + 100)
            ls_assets = list(df_p_pdg.columns.values)
            ls_assets.remove("class")
            if df_transit is not None:
                event_store.append(
                    __populate_event(df_transit, event_type + 100, "PDG", ls_assets, ["Pa"], data_set_id)
                )
            # Prepare DF for FAILURE EVENT
            df_failure = __get_event_df(df_p_pdg, event_type)
            if df_failure is not None:
                event_store.append(__populate_event(df_failure, event_type, "PDG", ls_assets, ["Pa"], data_set_id))

        print(f"The DF Size {str(df.shape)}")

        c.events.create(event_store)
        print(f"Done => {asset}")

    except (CogniteAPIError, ValueError, IndexError, CogniteException) as error:
        print(f"Error occured while creating events {error}")

        raise error
