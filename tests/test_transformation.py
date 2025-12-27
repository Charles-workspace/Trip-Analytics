from unittest.mock import MagicMock, patch
from src.trip_pipeline.transform.trip_data_transformer import trip_records_transformer
from src.trip_pipeline.transform.weather_data_transformer import pivot_weather_table
from trip_pipeline.configs.data_objects import config

@patch("src.trip_pipeline.transform.trip_data_transformer.to_timestamp")
@patch("src.trip_pipeline.transform.trip_data_transformer.to_date")
def test_trip_records_transformer(mock_to_date, mock_to_timestamp):
    # mock session + table
    session = MagicMock()
    df = MagicMock()
    final_df = MagicMock()

    session.table.return_value = df

    df.with_column.return_value = df
    df.with_column_renamed.return_value = df
    df.select.return_value = final_df

    result = trip_records_transformer(session,valid_trip_data=config.valid_trip_data)

    session.table.assert_called_once_with(config.valid_trip_data)

    # Verify expected transformation calls
    assert df.with_column.call_count >= 3  
    assert df.with_column_renamed.call_count >= 3  
    df.select.assert_called_once()

    mock_to_date.assert_called()
    mock_to_timestamp.assert_called()
    assert result == final_df

def test_pivot_weather_table():
    session = MagicMock()
    df = MagicMock()
    pivoted = MagicMock()
    final = MagicMock()

    # Mock session.table
    session.table.return_value = df

    # Mock datatype discovery
    df.select.return_value.distinct.return_value.collect.return_value = [("TMIN",),("TMAX",),("PRCP",)]

    # Mock pivot chain
    df.group_by.return_value.pivot.return_value.agg.return_value = pivoted

    pivoted.with_column.return_value = pivoted   # date -> o_date assign
    pivoted.select.return_value = final

    result = pivot_weather_table(session, "RAW_WEATHER")

    session.table.assert_called_once_with("RAW_WEATHER")
    df.group_by.assert_called_once_with("date")
    df.group_by.return_value.pivot.assert_called_once()
    df.group_by.return_value.pivot.return_value.agg.assert_called_once()
    pivoted.select.assert_called_once()

    assert result == final