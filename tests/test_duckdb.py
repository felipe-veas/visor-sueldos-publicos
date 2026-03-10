import duckdb
import tempfile
import os


def test_duckdb_parameterized_read_csv():
    """Test that DuckDB can read a CSV using parameterized paths to prevent SQL injection."""
    # Create a dummy CSV
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as tmp:
        tmp.write("col1;col2\nval1;val2\n")
        tmp_path = tmp.name

    try:
        # Test parameterized query
        df = duckdb.query(
            "SELECT * FROM read_csv(?, delim=';') LIMIT 0", params=[tmp_path]
        ).to_df()
        assert list(df.columns) == ["col1", "col2"]
    finally:
        os.remove(tmp_path)
