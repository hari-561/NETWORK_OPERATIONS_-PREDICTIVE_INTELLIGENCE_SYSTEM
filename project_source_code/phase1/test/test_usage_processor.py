import pandas as pd

from usage_processor import UsageProcessor


def test_load_data():

    df = pd.DataFrame({
        "timestamp": ["2013-11-01 00:00:00"],
        "grid_id": [1],
        "country_code": [0],
        "sms_in": [10],
        "sms_out": [5],
        "call_in": [2],
        "call_out": [3],
        "internet_activity": [20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    result = processor.load_data()

    assert len(result) == 1
    assert list(result.columns) == list(df.columns)



def test_clean_data_null_policy():

    df = pd.DataFrame({
        "timestamp": ["2013-11-01 00:00:00"],
        "grid_id": [1],
        "country_code": [0],
        "sms_in": [None],
        "sms_out": [5],
        "call_in": [None],
        "call_out": [3],
        "internet_activity": [20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    processor.load_data()
    result = processor.clean_data()

    assert result["sms_in"].iloc[0] == 0
    assert result["call_in"].iloc[0] == 0




def test_derive_time_features():

    df = pd.DataFrame({
        "timestamp": ["2013-11-01 14:00:00"],
        "grid_id": [1],
        "country_code": [0],
        "sms_in": [10],
        "sms_out": [5],
        "call_in": [2],
        "call_out": [3],
        "internet_activity": [20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    processor.load_data()
    processor.clean_data()

    result = processor.derive_time_features()

    assert result["hour"].iloc[0] == 14
    assert result["date"].iloc[0].isoformat() == "2013-11-01"
    assert result["day_of_week"].iloc[0] == "Friday"




def test_aggregate_to_grid_time():

    df = pd.DataFrame({
        "timestamp": [
            "2013-11-01 10:00:00",
            "2013-11-01 10:00:00"
        ],
        "grid_id": [1, 1],
        "country_code": [0, 33],
        "sms_in": [10, 20],
        "sms_out": [5, 5],
        "call_in": [2, 3],
        "call_out": [1, 2],
        "internet_activity": [10, 20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()

    result = processor.aggregate_to_grid_time()

    # Two country rows should become one grid/hour row
    assert len(result) == 1

    assert result["sms_in"].iloc[0] == 30
    assert result["internet_activity"].iloc[0] == 30

    # country_code must not exist in grid/hour output
    assert "country_code" not in result.columns




def test_derive_activity_features():

    df = pd.DataFrame({
        "timestamp": ["2013-11-01 10:00:00"],
        "grid_id": [1],
        "country_code": [0],
        "sms_in": [10],
        "sms_out": [5],
        "call_in": [2],
        "call_out": [3],
        "internet_activity": [20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()

    result = processor.derive_activity_features()

    assert result["total_sms"].iloc[0] == 15
    assert result["total_calls"].iloc[0] == 5
    assert result["total_activity"].iloc[0] == 40





def test_compute_kpis():

    df = pd.DataFrame({
        "timestamp": [
            "2013-11-01 10:00:00",
            "2013-11-01 11:00:00"
        ],
        "grid_id": [1, 1],
        "country_code": [0, 0],
        "sms_in": [10, 20],
        "sms_out": [5, 5],
        "call_in": [2, 3],
        "call_out": [1, 2],
        "internet_activity": [10, 20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()

    kpis = processor.compute_kpis()

    assert kpis["unique_grids"] == 1
    assert kpis["busiest_grid"] == 1
    assert kpis["total_sms"] == 45
    assert kpis["total_calls"] == 8




def test_export_summary(tmp_path):

    df = pd.DataFrame({
        "timestamp": ["2013-11-01 10:00:00"],
        "grid_id": [1],
        "country_code": [0],
        "sms_in": [10],
        "sms_out": [5],
        "call_in": [2],
        "call_out": [3],
        "internet_activity": [20]
    })

    processor = UsageProcessor(
        dataframe=df
    )

    processor.load_data()
    processor.clean_data()
    processor.derive_time_features()
    processor.aggregate_to_grid_time()
    processor.derive_activity_features()
    processor.compute_kpis()

    paths = processor.export_summary(
        tmp_path
    )

    assert paths["grid_hour"].exists()
    assert paths["daily"].exists()
    assert paths["grid"].exists()




