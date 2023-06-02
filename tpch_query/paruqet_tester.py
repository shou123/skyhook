import pyarrow.parquet as pq
import pandas as pd

try:
    # Use PyArrow to open the file
    parquet_file = pq.ParquetFile("/mnt/cephfs/dataset/nyc")
except Exception as e:
    print("This is not a valid Parquet file: ", e)
else:
    print("This is a valid Parquet file")
    
    row_group = parquet_file.read_row_group(0)
    # Convert the row group to a Pandas dataframe
    df = row_group.to_pandas()
    # Print the contents of the dataframe
    print(df)

    # data_frame = pd.DataFrame(df)
    # data_frame.to_csv('/home/yue21/scripts/parquet_verified_data.csv', index=False)