from src.trip_pipeline.dq.dq_check import DQCheck
from unittest.mock import MagicMock,patch
import importlib

def dq_module_path():
    return importlib.import_module("trip_pipeline.dq.dq_check").__name__


# Test case 1: Test null_check with rows containing null/empty values in key columns
def test_null_check():
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_null_table"

    dq = DQCheck(MagicMock(),key_columns, dq_table)

    mock_df = MagicMock()
    mock_invalid_df = MagicMock()
    mock_valid_df = MagicMock()

    mock_df.filter.side_effect = [mock_invalid_df, mock_valid_df]

    mock_invalid_df.count.return_value = 3
    mock_valid_df.count.return_value = 5

    write = MagicMock()
    mode = MagicMock()
    write.mode.return_value = mode
    mock_invalid_df.write = write

    with patch(f"{dq_module_path()}.DQCheck.build_null_condition") as mock_build_condition:
        count, valid_df = dq.null_check(mock_df, dq_table)

    assert count == 3
    assert valid_df is mock_valid_df
    write.mode.assert_called_once_with("overwrite")
    mode.save_as_table.assert_called_once_with(dq_table)

    
# Test case 2: Test null_check with no rows containing null/empty values in key columns

def test_null_check_no_nulls():
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_null_table"

    dq = DQCheck(MagicMock(),key_columns, dq_table)

    mock_df = MagicMock()
    mock_invalid_df = MagicMock()
    mock_valid_df = MagicMock()

    mock_df.filter.side_effect = [mock_invalid_df, mock_valid_df]

    mock_invalid_df.count.return_value = 0

    write = MagicMock()
    mock_invalid_df.write = write

    with patch(f"{dq_module_path()}.DQCheck.build_null_condition") as mock_build_condition:
        count, valid_df = dq.null_check(mock_df, dq_table)
        
    assert count == 0
    assert valid_df is mock_valid_df
    write.mode.assert_not_called()

# Test case 3: Test duplicate_check with duplicate rows based on key columns

def test_duplicate_check_with_duplicates():
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_duplicate_table"

    mock_session = MagicMock()
    mock_df = MagicMock()

    df_with_rn = MagicMock()
    df_clean = MagicMock()
    df_dupes_base = MagicMock()
    df_dupes_final = MagicMock()

    dq = DQCheck(mock_session, key_columns, dq_table)

    # Chain the .with_column call
    mock_df.with_column.return_value = df_with_rn

    # Simulate .filter(col('rn') == 1) returns clean rows
    df_with_rn.filter.side_effect = [df_clean, df_dupes_base]  # First for clean, second for dupes

    # Simulate dropping rn
    df_clean.drop.return_value = df_clean
    df_dupes_base.drop.return_value = df_dupes_base

    # simulate with_column("dq_ts")
    df_dupes_base.with_column.return_value = df_dupes_final

    df_dupes_final.count.return_value = 2


    # Setup write mocks
    write = MagicMock()
    mode = MagicMock()
    write.mode.return_value = mode
    df_dupes_final.write = write

    # Patch col and row_number if needed (optional here since we don't evaluate expressions)

    result = dq.duplicate_check(mock_df, dq_table)

    assert result == df_clean
    df_dupes_final.count.assert_called_once()
    write.mode.assert_called_once_with("overwrite")
    mode.save_as_table.assert_called_once_with(dq_table)

# Test case 4: Test duplicate_check with no duplicate rows

def test_duplicate_check_no_duplicates():
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_duplicate_table"

    mock_session = MagicMock()
    mock_df = MagicMock()

    df_with_rn = MagicMock()
    df_clean = MagicMock()
    df_dupes_base = MagicMock()
    df_dupes_final = MagicMock()

    dq = DQCheck(mock_session, key_columns, dq_table)

    # Chain the .with_column call
    mock_df.with_column.return_value = df_with_rn

    # Simulate .filter(col('rn') == 1) returns clean rows
    df_with_rn.filter.side_effect = [df_clean, df_dupes_base]  # First for clean, second for dupes

    # Simulate dropping rn
    df_clean.drop.return_value = df_clean
    df_dupes_base.drop.return_value = df_dupes_base

    df_dupes_base.with_column.return_value = df_dupes_final

    # Simulate counts
    df_dupes_final.count.return_value = 0
  
    # Setup write mocks
    write = MagicMock()
    df_dupes_final.write = write

    # Patch col and row_number if needed (optional here since we don't evaluate expressions)
    result = dq.duplicate_check(mock_df, dq_table)

    assert result == df_clean
    df_dupes_final.count.assert_called_once()
    write.mode.assert_not_called()

# Test case 5: Test validate trip data with junk values

def test_validate_trip_data_returns_junk_and_clean_dfs():
    dq = DQCheck(MagicMock(), ["pickup_time"], "dq_table")

    mock_df = MagicMock()
    junk_df = MagicMock()
    clean_df = MagicMock()

    mock_df.filter.side_effect = [junk_df, clean_df]

    result_junk, result_clean = dq.validate_trip_data(
        mock_df,
        columns=["pickup_time"],
        dtype="timestamp_type",
        min_valid_epoch=1600000000000
    )

    assert result_junk is junk_df
    assert result_clean is clean_df
    assert mock_df.filter.call_count == 2

# Test case 6: Test validate weather data with junk values

def test_validate_weather_data_datatype_check():
    dq = DQCheck(MagicMock(), ["datatype"], "dq_table")

    mock_df = MagicMock()
    junk_df = MagicMock()
    clean_df = MagicMock()

    mock_df.filter.side_effect = [junk_df, clean_df]

    result_junk, result_clean = dq.validate_weather_data(
        mock_df,
        columns=["datatype"],
        dtype="datatype_check",
        weather_data_types=["TMIN", "TMAX"]
    )

    assert result_junk is junk_df
    assert result_clean is clean_df
    assert mock_df.filter.call_count == 2

    # Check for error on unsupported dtype

def test_validate_weather_data_invalid_dtype_raises():
    dq = DQCheck(MagicMock(), ["col"], "dq_table")
    mock_df = MagicMock()

    try:
        dq.validate_weather_data(
            mock_df,
            columns=["col"],
            dtype="invalid_type",
            weather_data_types=[]
        )
        assert False, "Expected ValueError was not raised"
    except ValueError as e:
        assert "Unsupported dtype" in str(e)