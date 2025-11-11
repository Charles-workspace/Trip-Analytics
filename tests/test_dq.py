from src.dq.dq_check import DQCheck
from unittest.mock import MagicMock,patch


# Test case 1: Test null_check with rows containing null/empty values in key columns
def test_null_check():
    key_columns = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
    dq_table = "test_null_table"

    mock_session = MagicMock()
    mock_df = MagicMock()
    mock_invalid_df = MagicMock()
    mock_valid_df = MagicMock()

    dq = DQCheck(mock_session,key_columns, dq_table)

    # Create a fake Column object for the condition
    fake_condition = MagicMock(name="fake_condition")
    negated_condition = MagicMock(name="negated_condition")

    # Simulate ~cond as returning another mock
    fake_condition.__invert__.return_value = negated_condition

    # Setup .filter() to return different mocks based on input condition identity
    def filter_side_effect(cond):
        cond_str = str(cond)
        if "IS NULL" in cond_str:
            return mock_invalid_df  
        elif "NOT" in cond_str or "~" in cond_str:
            return mock_valid_df    #
        else:
            print(f"[DEBUG] Unexpected condition: {cond}")
            raise ValueError("Unexpected condition")

    mock_df.filter.side_effect = filter_side_effect


    mock_invalid_df.count.return_value = 3
    mock_valid_df.count.return_value = 3
    mock_invalid_df.write.mode.return_value.save_as_table = MagicMock()

    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_write.mode.return_value = mock_mode
    mock_mode.save_as_table.return_value = None
    mock_invalid_df.write = mock_write


    with patch("dq.dq_check.DQCheck.build_null_condition") as mock_build_condition:
        mock_condition = MagicMock(name="condition")
        mock_build_condition.return_value = mock_condition
        count, valid_df = dq.null_check(mock_df, dq_table)

    print(mock_invalid_df.write.mode.call_args_list)
    print(mock_invalid_df.write.mode.return_value.save_as_table.call_args_list)

    assert count == 3
    assert isinstance(valid_df, MagicMock)

    mock_write.mode.assert_called_once_with("overwrite")
    mock_mode.save_as_table.assert_called_once_with(dq_table)
    
# Test case 2: Test null_check with no rows containing null/empty values in key columns

def test_null_check_no_nulls():
    key_columns = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"]
    dq_table = "test_null_table"

    mock_session = MagicMock()
    mock_df = MagicMock()
    mock_invalid_df = MagicMock()
    mock_valid_df = MagicMock()

    dq = DQCheck(mock_session,key_columns, dq_table)

    # Create a fake Column object for the condition
    fake_condition = MagicMock(name="fake_condition")
    negated_condition = MagicMock(name="negated_condition")

    # Simulate ~cond as returning another mock
    fake_condition.__invert__.return_value = negated_condition

    # Setup .filter() to return different mocks based on input condition identity
    def filter_side_effect(cond):
        cond_str = str(cond)
        if "IS NULL" in cond_str:
            return mock_invalid_df  
        elif "NOT" in cond_str or "~" in cond_str:
            return mock_valid_df    #
        else:
            print(f"[DEBUG] Unexpected condition: {cond}")
            raise ValueError("Unexpected condition")

    mock_df.filter.side_effect = filter_side_effect


    mock_invalid_df.count.return_value = 0
    mock_valid_df.count.return_value = 6
    mock_invalid_df.write.mode.return_value.save_as_table = MagicMock()

    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_write.mode.return_value = mock_mode
    mock_mode.save_as_table.return_value = None
    mock_invalid_df.write = mock_write


    with patch("dq.dq_check.DQCheck.build_null_condition") as mock_build_condition:
        mock_condition = MagicMock(name="condition")
        mock_build_condition.return_value = mock_condition
        count, valid_df = dq.null_check(mock_df, dq_table)

    print(mock_invalid_df.write.mode.call_args_list)
    print(mock_invalid_df.write.mode.return_value.save_as_table.call_args_list)

    assert count == 0
    assert isinstance(valid_df, MagicMock)

    mock_write.mode.assert_not_called()
    mock_mode.save_as_table.assert_not_called()

# Test case 3: Test duplicate_check with duplicate rows based on key columns

def test_duplicate_check_with_duplicates():
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_duplicate_table"

    mock_session = MagicMock()
    mock_df = MagicMock()
    mock_df_with_rn = MagicMock()
    mock_clean_df = MagicMock()
    mock_dupe_df = MagicMock()

    dq = DQCheck(mock_session, key_columns, dq_table)

    # Chain the .with_column call
    mock_df.with_column.return_value = mock_df_with_rn

    # Simulate .filter(col('rn') == 1) returns clean rows
    mock_df_with_rn.filter.side_effect = [mock_clean_df, mock_dupe_df]  # First for clean, second for dupes

    # Simulate dropping rn
    mock_clean_df.drop.return_value = mock_clean_df
    mock_dupe_df.drop.return_value = mock_dupe_df

    # Simulate counts
    mock_clean_df.count.return_value = 3
    mock_dupe_df.count.return_value = 2

    # Setup write mocks
    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_write.mode.return_value = mock_mode
    mock_mode.save_as_table.return_value = None
    mock_dupe_df.write = mock_write

    # Patch col and row_number if needed (optional here since we don't evaluate expressions)

    result = dq.duplicate_check(mock_df, dq_table)

    assert result == mock_clean_df
    mock_df.with_column.assert_called_once()
    assert mock_clean_df.count.called
    assert mock_dupe_df.count.called
    mock_write.mode.assert_called_once_with("overwrite")
    mock_mode.save_as_table.assert_called_once_with(dq_table)

# Test case 4: Test duplicate_check with no duplicate rows

def test_duplicate_check_no_duplicates():
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_duplicate_table"

    mock_session = MagicMock()
    mock_df = MagicMock()
    mock_df_with_rn = MagicMock()
    mock_clean_df = MagicMock()
    mock_dupe_df = MagicMock()

    dq = DQCheck(mock_session, key_columns, dq_table)

    # Chain the .with_column call
    mock_df.with_column.return_value = mock_df_with_rn

    # Simulate .filter(col('rn') == 1) returns clean rows
    mock_df_with_rn.filter.side_effect = [mock_clean_df, mock_dupe_df]  # First for clean, second for dupes

    # Simulate dropping rn
    mock_clean_df.drop.return_value = mock_clean_df
    mock_dupe_df.drop.return_value = mock_dupe_df

    # Simulate counts
    mock_clean_df.count.return_value = 3
    mock_dupe_df.count.return_value = 0

    # Setup write mocks
    mock_write = MagicMock()
    mock_mode = MagicMock()
    mock_write.mode.return_value = mock_mode
    mock_mode.save_as_table.return_value = None
    mock_dupe_df.write = mock_write

    # Patch col and row_number if needed (optional here since we don't evaluate expressions)

    result = dq.duplicate_check(mock_df, dq_table)

    assert result == mock_clean_df
    mock_df.with_column.assert_called_once()
    assert mock_clean_df.count.called
    assert mock_dupe_df.count.called

    mock_write.mode.assert_not_called()
    mock_mode.save_as_table.assert_not_called()

# Test case 5: Test junk_value_check with junk values in key columns

def test_junk_value_check_with_junk():
    key_columns = ["pickup_time", "dropoff_time"]
    dq_table = "test_junk_check"
    min_valid_epoch = 1600000000000
    dtype = "timestamp_type"

    mock_session = MagicMock()
    dq = DQCheck(mock_session, key_columns, dq_table)

    mock_df = MagicMock()
    mock_junk_df = MagicMock()
    mock_clean_df = MagicMock()

    # Setup return values for .filter
    # We're not testing filter logic itself, so we mock the outcome of filter()
    mock_df.filter.side_effect = [mock_junk_df, mock_clean_df]

    # Setup return values for count (optional)
    mock_junk_df.count.return_value = 2
    mock_clean_df.count.return_value = 3

    # Patch the try_cast and col used inside the method
    with patch("dq.dq_check.col", side_effect=lambda x: x), \
         patch("dq.dq_check.trim", side_effect=lambda x: x), \
         patch("dq.dq_check.try_cast", side_effect=lambda x, _: x):

        junk_df, clean_df = dq.junk_value_check(mock_df, key_columns, dtype, min_valid_epoch)

    assert junk_df.count() == 2
    assert clean_df.count() == 3
    assert isinstance(junk_df, MagicMock)
    assert isinstance(clean_df, MagicMock)

# Test case 6: Test junk_value_check with no junk values in key columns

def test_junk_value_check_no_junk():
    key_columns = ["pickup_time", "dropoff_time"]
    dq_table = "test_junk_check"
    min_valid_epoch = 1600000000000
    dtype = "timestamp_type"

    mock_session = MagicMock()
    dq = DQCheck(mock_session, key_columns, dq_table)

    mock_df = MagicMock()
    mock_junk_df = MagicMock()
    mock_clean_df = MagicMock()

    # Setup return values for .filter
    mock_df.filter.side_effect = [mock_junk_df, mock_clean_df]

    mock_junk_df.count.return_value = 0
    mock_clean_df.count.return_value = 5

    with patch("dq.dq_check.col", side_effect=lambda x: x), \
         patch("dq.dq_check.trim", side_effect=lambda x: x), \
         patch("dq.dq_check.try_cast", side_effect=lambda x, _: x):

        junk_df, clean_df = dq.junk_value_check(mock_df, key_columns, dtype, min_valid_epoch)

    assert junk_df.count() == 0
    assert clean_df.count() == 5
    assert isinstance(junk_df, MagicMock)
    assert isinstance(clean_df, MagicMock)

# Test case 7: Test weather_junk_val_check with junk values in weather data

def test_weather_junk_val_check_timestamp_junk():
    columns = ["date"]
    dtype = "timestamp_type"
    weather_data_types = []  # not used in this case

    mock_session = MagicMock()
    dq = DQCheck(mock_session, columns, dq_table_name="dummy")

    mock_df = MagicMock()
    mock_junk_df = MagicMock()
    mock_clean_df = MagicMock()

    mock_df.filter.side_effect = [mock_junk_df, mock_clean_df]
    mock_junk_df.count.return_value = 1
    mock_clean_df.count.return_value = 5

    with patch("dq.dq_check.col", side_effect=lambda x: x), \
         patch("dq.dq_check.trim", side_effect=lambda x: x), \
         patch("dq.dq_check.try_cast", side_effect=lambda x, _: x):

        junk_df, clean_df = dq.weather_junk_val_check(mock_df, columns, dtype, weather_data_types)

    assert junk_df.count() == 1
    assert clean_df.count() == 5
    assert isinstance(junk_df, MagicMock)
    assert isinstance(clean_df, MagicMock)

# Test case 8: Test weather_junk_val_check with junk values in datatype column

def test_weather_junk_val_check_datatype_junk():
    columns = ["datatype"]
    dtype = "datatype_check"
    weather_data_types = ["AWND", "WT01", "WSF5", "WSF2", "WDF5", "WDF2", "TMIN", "TMAX", "SNWD", "PRCP", "WT08", "SNOW", "WT03", "WT02"]

    mock_session = MagicMock()
    dq = DQCheck(mock_session, columns, dq_table_name="dummy")

    mock_df = MagicMock()
    mock_junk_df = MagicMock()
    mock_clean_df = MagicMock()

    mock_df.filter.side_effect = [mock_junk_df, mock_clean_df]
    mock_junk_df.count.return_value = 2
    mock_clean_df.count.return_value = 4

    with patch("dq.dq_check.col", side_effect=lambda x: x), \
         patch("dq.dq_check.trim", side_effect=lambda x: x), \
         patch("dq.dq_check.try_cast", side_effect=lambda x, _: x):  # still patch just in case

        junk_df, clean_df = dq.weather_junk_val_check(mock_df, columns, dtype, weather_data_types)

    assert junk_df.count() == 2
    assert clean_df.count() == 4
    assert isinstance(junk_df, MagicMock)
    assert isinstance(clean_df, MagicMock)

# Test case 9: Test weather_junk_val_check with no junk values

def test_weather_junk_val_check_with_junk_timestamps():
    mock_session = MagicMock()
    dq = DQCheck(mock_session, [], "")  # key columns and dq_table_name not needed here

    mock_df = MagicMock()
    mock_junk_df = MagicMock()
    mock_clean_df = MagicMock()

    # Simulate filter returning junk and clean
    mock_df.filter.side_effect = [mock_junk_df, mock_clean_df]
    mock_junk_df.count.return_value = 2
    mock_clean_df.count.return_value = 3

    with patch("dq.dq_check.trim", side_effect=lambda col_obj: col_obj), \
         patch("dq.dq_check.col", side_effect=lambda x: f"col({x})"), \
         patch("dq.dq_check.try_cast", side_effect=lambda col_obj, dtype: f"casted({col_obj})"), \
         patch("dq.dq_check.TimestampType"):  # dummy patch to avoid errors
         
        junk_df, clean_df = dq.weather_junk_val_check(mock_df, ['date'], "timestamp_type", [])

    assert junk_df == mock_junk_df
    assert clean_df == mock_clean_df
    assert mock_junk_df.count() == 2
    assert mock_clean_df.count() == 3

# Test case 10: Test weather_junk_val_check with no junk values

def test_weather_junk_val_check_no_junk_timestamps():
    mock_session = MagicMock()
    dq = DQCheck(mock_session, [], "")

    mock_df = MagicMock()
    mock_junk_df = MagicMock()
    mock_clean_df = MagicMock()

    # Simulate no junk: junk_df count = 0
    mock_df.filter.side_effect = [mock_junk_df, mock_clean_df]
    mock_junk_df.count.return_value = 0
    mock_clean_df.count.return_value = 5

    with patch("dq.dq_check.trim", side_effect=lambda col_obj: col_obj), \
         patch("dq.dq_check.col", side_effect=lambda x: f"col({x})"), \
         patch("dq.dq_check.try_cast", side_effect=lambda col_obj, dtype: f"casted({col_obj})"), \
         patch("dq.dq_check.TimestampType"):
        
        junk_df, clean_df = dq.weather_junk_val_check(mock_df, ['date'], "timestamp_type", [])

    assert junk_df == mock_junk_df
    assert clean_df == mock_clean_df
    assert mock_junk_df.count() == 0
    assert mock_clean_df.count() == 5