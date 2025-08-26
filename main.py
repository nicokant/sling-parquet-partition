import duckdb
from sling import Replication, ReplicationStream, Mode
import pyarrow.parquet as pq
from pyarrow.fs import LocalFileSystem, FileSelector, FileType
from itertools import cycle

# synthetic data
DATA_QUERY = """
CREATE TABLE data AS
SELECT
    "range" as "date",
    gen_random_uuid() as id,
FROM
    range (
        DATE '2025-01-01',
        DATE '2025-12-31',
        INTERVAL '15' DAY
    )
"""


def create_data():
    conn = duckdb.connect()
    conn.sql(DATA_QUERY)

    # Create a simple parquet out of that
    conn.sql('copy data to "data.parquet"')

    # Create a partitioned version of the same parquet
    conn.sql("""
        copy (
            select 
                *, 
                date_part('year', "date")::text as year,
                lpad(date_part('month', "date")::text, 2, '0') as month, 
                lpad(date_part('day', "date")::text, 2, '0') as day,  
            from data
        ) to 'duckdb_partitioned' (format parquet, partition_by (year, month, day), overwrite)
    """)

    Replication(
        debug=True,
        source="file://",
        target="file://",
        streams={
            "data.parquet": ReplicationStream(
                mode=Mode.FULL_REFRESH,
                object="sling_partitioned/{part_year}/{part_month}/{part_day}",
                update_key="date",
                target_options={"format": "parquet"},
            )
        },
    ).run()


def check_data():
    FILE = "data"

    duckdb_files = cycle(
        [
            info
            for info in (
                LocalFileSystem().get_file_info(
                    FileSelector("duckdb_partitioned", recursive=True)
                )
            )
            if info.type == FileType.File
        ]
    )
    sling_files = cycle(
        [
            info
            for info in (
                LocalFileSystem().get_file_info(
                    FileSelector("sling_partitioned", recursive=True)
                )
            )
            if info.type == FileType.File
        ]
    )

    while True:
        duckdb_parquet = pq.ParquetFile(next(duckdb_files).path)
        sling_parquet = pq.ParquetFile(next(sling_files).path)

        try:
            assert duckdb_parquet.schema.equals(sling_parquet.schema)
        except AssertionError:
            print(duckdb_parquet.schema, sling_parquet.schema)

    # for info in file_infos:
    #     if :
    #         print(info.path)
    #         pf = pq.ParquetFile(info.path)
    #         schema = pf.schema
    #         print(schema)
    #         s = schema.to_arrow_schema()

    #         break


if __name__ == "__main__":
    create_data()
    check_data()
