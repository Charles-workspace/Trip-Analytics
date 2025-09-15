
import os
from types import SimpleNamespace

# Must set **all required env vars** before importing anything that depends on them
os.environ["SNOWFLAKE_ACCOUNT_NAME"] = "dummy_account"
os.environ["SNOWFLAKE_USER"] = "dummy_user"
os.environ["SNOWFLAKE_PASSWORD"] = "dummy_password"
os.environ["SNOWFLAKE_ROLE"] = "dummy_role"


from unittest.mock import MagicMock, patch
from src.transform.trip_data_transformer import trip_records_transformer

@patch("src.transform.trip_data_transformer.to_date")
@patch("src.transform.trip_data_transformer.to_timestamp")
@patch("src.transform.trip_data_transformer.Session")
def test_trip_records_transformer(mock_session_class, mock_to_timestamp, mock_to_date):
    # Setup mock session and mock table return
    mock_session = MagicMock()
    mock_df = MagicMock()
    mock_transformed_df = MagicMock()
    mock_final_df = MagicMock()

    # Patch Session.builder.configs().create()
    mock_session_class.builder.configs.return_value.create.return_value = mock_session
    mock_session.table.return_value = mock_df

    # Mock transformations
    mock_df.with_column.return_value = mock_df  # Chaining
    mock_df.with_column_renamed.return_value = mock_df  # Chaining
    mock_df.select.return_value = mock_final_df

    # Call function
    result = trip_records_transformer()

    # Assertions
    mock_session.table.assert_called_once()
    mock_df.with_column.assert_any_call('"PickupTime"', mock_to_timestamp.return_value)
    mock_df.with_column.assert_any_call('"DropoffTime"', mock_to_timestamp.return_value)
    mock_df.with_column.assert_any_call('"RideDate"', mock_to_date.return_value)
    mock_df.select.assert_called_once_with(
        '"VendorID"', '"RideDate"', '"PickupTime"', '"DropoffTime"', '"TripDistance"',
        '"PULocationID"', '"DOLocationID"', '"FareAmount"', '"TipAmount"',
        '"TollsAmount"', '"TotalAmount"'
    )
    assert result == mock_final_df


from unittest.mock import MagicMock, patch
from src.transform.weather_data_transformer import pivot_weather_table

def test_pivot_weather_table():
    # Mock all external dependencies
    mock_session = MagicMock()
    mock_df = MagicMock()
    mock_pivoted_df = MagicMock()
    mock_renamed_df = MagicMock()
    mock_selected_df = MagicMock()
    
    fake_datatypes = [("TMIN",), ("TMAX",), ("PRCP",)]  # simulate .collect() returning a list of tuples

    with patch("src.transform.weather_data_transformer.Session") as mock_session_class, \
     patch("src.transform.weather_data_transformer.connection_parameters", SimpleNamespace(mock_key="mock_val")), \
     patch("src.transform.weather_data_transformer.min") as mock_min, \
     patch("src.transform.weather_data_transformer.col") as mock_col, \
     patch("src.transform.weather_data_transformer.to_date") as mock_to_date:

        # Patch the session and data retrieval
        mock_session_class.builder.configs.return_value.create.return_value = mock_session
        mock_session.table.return_value = mock_df
        mock_df.select.return_value.distinct.return_value.collect.return_value = fake_datatypes

        # Mock group_by().pivot().agg() chain
        mock_df.group_by.return_value.pivot.return_value.agg.return_value = mock_pivoted_df

        # Chain column operations
        mock_pivoted_df.with_column.return_value = mock_renamed_df
        mock_renamed_df.with_column_renamed.return_value = mock_renamed_df  # allow chaining multiple renames
        mock_renamed_df.select.return_value = mock_selected_df

        # Run the function
        result = pivot_weather_table()

        # Assertions
        assert result == mock_selected_df
        mock_session.table.assert_called_once()
        mock_df.select.assert_called_once_with("datatype")
        mock_df.group_by.assert_called_once_with("DATE")
        mock_pivoted_df.with_column.assert_called()
        mock_renamed_df.select.assert_called_once()