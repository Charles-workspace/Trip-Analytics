from src.trip_pipeline.dq.dq_check import DQCheck
from unittest.mock import MagicMock,patch
import importlib
from snowflake.snowpark.functions import lit 

def dq_module_path():
    return importlib.import_module("trip_pipeline.dq.dq_check").__name__

def test_null_check():
    # 1. Setup
    key_columns = ["VendorID", "tpep_pickup_datetime"]
    dq_table = "test_null_table"
    
    mock_session = MagicMock()
    dq = DQCheck(mock_session, key_columns)

    mock_df = MagicMock()
    mock_invalid_df = MagicMock()
    mock_valid_df = MagicMock()
    mock_df_after_metadata = MagicMock()
    
    # FIX: Use a real Snowpark Column object instead of a MagicMock
    # lit(True) creates a Column object that won't trigger the AST TypeError
    mock_condition = lit(True)

    # side_effect for the two filter calls (invalid, then valid)
    mock_df.filter.side_effect = [mock_invalid_df, mock_valid_df]
    
    # Configure counts
    mock_df.count.return_value = 10
    mock_invalid_df.count.return_value = 3
    mock_valid_df.count.return_value = 7

    # 2. Execution with Patches
    # Ensure the path matches where DQCheck and add_etl_metadata_dq are
    with patch(f"{dq_module_path()}.DQCheck.build_null_condition", return_value=mock_condition), \
         patch("src.trip_pipeline.dq.dq_check.add_etl_metadata_dq", return_value=mock_df_after_metadata):
        
        result = dq.null_check(mock_df, dq_table)

    # 3. Assertions
    assert result is mock_valid_df
    
    # Verify write chain on the metadata-enriched dataframe
    mock_df_after_metadata.write.mode.assert_called_once_with("append")
    mock_df_after_metadata.write.mode.return_value.save_as_table.assert_called_once_with(dq_table)

# Test case 2: Test null_check with no rows containing null/empty values in key columns

def test_null_check_no_nulls():
    # 1. Setup
    key_columns = ["VendorID"]
    dq_table = "test_null_table"
    dq = DQCheck(MagicMock(), key_columns)

    mock_df = MagicMock()
    mock_invalid_df = MagicMock()
    mock_valid_df = MagicMock()
    
    # FIX: Use lit(True) to avoid the AST TypeError with the ~ operator
    mock_condition = lit(True)

    # side_effect: 1st filter returns invalid (empty), 2nd returns valid (all records)
    mock_df.filter.side_effect = [mock_invalid_df, mock_valid_df]

    # Set count to 0 to skip the 'if invalid_count > 0' block
    mock_invalid_df.count.return_value = 0

    # 2. Execution
    with patch(f"{dq_module_path()}.DQCheck.build_null_condition", return_value=mock_condition):
        result = dq.null_check(mock_df, dq_table)
        
    # 3. Assertions
    assert result is mock_valid_df
    
    # Verify that we never tried to write to the DQ table
    mock_invalid_df.write.mode.assert_not_called()

# Test case 3: Test duplicate_check with duplicate rows based on key columns

def test_duplicate_check_with_duplicates():
    # 1. Setup
    key_columns = ["VendorID"]
    dq_table = "test_duplicate_table"
    dq = DQCheck(MagicMock(), key_columns)

    mock_df = MagicMock()
    df_with_rn = MagicMock()
    df_clean = MagicMock()
    df_dupes_base = MagicMock()
    df_dupes_after_metadata = MagicMock()

    # 2. Configure Chained Mock Returns
    mock_df.with_column.return_value = df_with_rn
    
    # .filter() is called twice in duplicate_check
    df_with_rn.filter.side_effect = [df_clean, df_dupes_base]
    
    # .drop() returns the same mock to allow further chaining
    df_clean.drop.return_value = df_clean
    df_dupes_base.drop.return_value = df_dupes_base

    # Mock counts to trigger the 'if dupe_count > 0' logic
    df_with_rn.count.return_value = 10
    df_dupes_base.count.return_value = 2
    df_clean.count.return_value = 8

    # 3. Patching
    # We use lit("dummy") because it returns a REAL Snowpark Column object.
    # This satisfies the "Column constructor only accepts str or expression" check.
    with patch("src.trip_pipeline.dq.dq_check.col", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.row_number", return_value=MagicMock()), \
         patch("src.trip_pipeline.dq.dq_check.Window", return_value=MagicMock()), \
         patch("src.trip_pipeline.dq.dq_check.add_etl_metadata_dq", return_value=df_dupes_after_metadata):
        
        result = dq.duplicate_check(mock_df, dq_table)

    # 4. Assertions
    assert result is df_clean
    
    # Verify the write call on the dataframe returned by add_etl_metadata_dq
    df_dupes_after_metadata.write.mode.assert_called_once_with("append")
    df_dupes_after_metadata.write.mode.return_value.save_as_table.assert_called_once_with(dq_table)

# Test case 4: Test duplicate_check with no duplicate rows

def test_duplicate_check_no_duplicates():
    # 1. Setup
    key_columns = ["VendorID"]
    dq_table = "test_duplicate_table"
    dq = DQCheck(MagicMock(), key_columns)

    mock_df = MagicMock()
    df_with_rn = MagicMock()
    df_clean = MagicMock()
    df_dupes_base = MagicMock()

    # Chain: df.with_column('rn', ...)
    mock_df.with_column.return_value = df_with_rn

    # .filter() calls: 1st for clean (rn==1), 2nd for dupes (rn!=1)
    df_with_rn.filter.side_effect = [df_clean, df_dupes_base]

    # .drop('rn') 
    df_clean.drop.return_value = df_clean
    df_dupes_base.drop.return_value = df_dupes_base

    # Simulate counts: No duplicates found
    df_with_rn.count.return_value = 10
    df_dupes_base.count.return_value = 0
    df_clean.count.return_value = 10

    # 2. Execution with Patches
    # We patch the Snowpark functions to avoid "TypeError: Column constructor only accepts str or expression"
    with patch("src.trip_pipeline.dq.dq_check.col", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.row_number", return_value=MagicMock()), \
         patch("src.trip_pipeline.dq.dq_check.Window", return_value=MagicMock()):
        
        result = dq.duplicate_check(mock_df, dq_table)

    # 3. Assertions
    assert result is df_clean
    
    # Ensure write was never called since dupe_count == 0
    df_dupes_base.write.mode.assert_not_called()
# Test case 5: Test validate trip data with junk values

from unittest.mock import MagicMock, patch
from snowflake.snowpark.functions import lit
from src.trip_pipeline.dq.dq_check import DQCheck

def test_validate_trip_data_returns_junk_and_clean_dfs():
    # 1. Setup
    dq = DQCheck(MagicMock(), ["pickup_time"])
    mock_df = MagicMock()
    junk_df = MagicMock()
    clean_df = MagicMock()
    
    # This mock represents the result of the metadata utility
    junk_df_after_metadata = MagicMock()

    # filter is called twice inside the loop for each column
    mock_df.filter.side_effect = [junk_df, clean_df]

    # 2. Patching
    # We patch the functions used to build 'junk_condition'
    with patch("src.trip_pipeline.dq.dq_check.col", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.trim", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.try_cast", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.add_etl_metadata_dq", return_value=junk_df_after_metadata):

        result_junk, result_clean = dq.validate_trip_data(
            mock_df,
            columns=["pickup_time"],
            dtype="timestamp_type"
        )

    # 3. Assertions
    # Note: result_junk will be junk_df_after_metadata because the code 
    # reassigns invalid_df = add_etl_metadata_dq(...)
    assert result_junk is junk_df_after_metadata
    assert result_clean is clean_df
    assert mock_df.filter.call_count == 2

# Test case 6: Test validate weather data with junk values

from unittest.mock import MagicMock, patch
from snowflake.snowpark.functions import lit
from src.trip_pipeline.dq.dq_check import DQCheck

def test_validate_weather_data_datatype_check():
    # 1. Setup
    dq = DQCheck(MagicMock(), ["datatype"])
    mock_df = MagicMock()
    junk_df_base = MagicMock()
    clean_df = MagicMock()
    
    # This is what the function actually returns after metadata is added
    junk_df_final = MagicMock()

    # .filter() is called twice (once for junk, once for clean)
    mock_df.filter.side_effect = [junk_df_base, clean_df]

    # 2. Patching
    # Patch 'col' and 'trim' so they return a real Snowpark Column (lit)
    # Patch 'add_etl_metadata_dq' so it returns our 'final' mock
    with patch("src.trip_pipeline.dq.dq_check.col", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.trim", return_value=lit("dummy")), \
         patch("src.trip_pipeline.dq.dq_check.add_etl_metadata_dq", return_value=junk_df_final):

        result_junk, result_clean = dq.validate_weather_data(
            mock_df,
            columns=["datatype"],
            dtype="datatype_check",
            weather_data_types=["TMIN"]
        )

    # 3. Assertions
    # result_junk is now junk_df_final because of the metadata re-assignment
    assert result_junk is junk_df_final
    assert result_clean is clean_df
    assert mock_df.filter.call_count == 2