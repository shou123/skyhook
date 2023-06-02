import os
import sys
import time
import json
import datetime

import multiprocessing as mp

import duckdb
import pyarrow.dataset as ds
import subprocess


def drop_caches():
    os.system("sync")
    os.system("echo 1 >/proc/sys/vm/drop_caches")
    os.system("echo 2 > /proc/sys/vm/drop_caches")
    os.system("echo 3 > /proc/sys/vm/drop_caches")
    os.system("sync")
    time.sleep(2)


def run_query(query, format, query_no):
    drop_caches()
    process_id = os.getpid()

    conn = duckdb.connect()
    s = time.time()
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    result = conn.execute(query).fetchall()
    e = time.time()
    conn.close()

    latencies = e - s

    log_str = f"{{\"query_no\":{query_no}}}|{{\"process_id\":{process_id}}}|{{\"latencies\":{latencies}}}|{{\"start_time\":{time_stamp}}}"
    # {{\"start_time\":{s}}}|{{\"end_time\":{e}}}"
    print(log_str)

    file_size = int(os.path.getsize(dataset_path))/1024/1024/1014 #get the file size with GB
    bandwidth = file_size/latencies

    return {
        "query": query_no,
        "format": format,
        "latency": latencies,
        "bandwidth": bandwidth,
        "process_id":process_id
    }


if __name__ == "__main__":
    dataset_path = str(sys.argv[1])
    if ',' in sys.argv[2]:
        query_nos = (sys.argv[2].split(','))  # List of query numbers
    else:
        query_nos = sys.argv[2]
        
    # formats = "parquet"  # List of formats
    # format_ = ds.SkyhookFileFormat( "parquet", "/etc/ceph/ceph.conf" )
    # formats = "skyhook"  # List of formats
    # print(format)


    # Path to the compiled binary
    binary_path = "/home/yue21/skyhookdm/scripts/example"
    # Command to execute the binary and capture its output
    command = [binary_path]
    # Execute the command and capture the output
    formats = subprocess.check_output(command).decode("utf-8")
    # Print the output
    print(formats)



    range_size = 2

    data = []

    lineitem = ds.dataset(os.path.join(dataset_path, "lineitem"), format=formats)
    supplier = ds.dataset(os.path.join(dataset_path, "supplier"), format=formats)
    customer = ds.dataset(os.path.join(dataset_path, "customer"), format=formats)
    region   = ds.dataset(os.path.join(dataset_path, "region"), format=formats)
    nation   = ds.dataset(os.path.join(dataset_path, "nation"), format=formats)
    orders   = ds.dataset(os.path.join(dataset_path, "orders"), format=formats)
    part     = ds.dataset(os.path.join(dataset_path, "part"), format=formats)
    partsupp = ds.dataset(os.path.join(dataset_path, "partsupp"), format=formats)

    processes = []

    for query_no in query_nos:
        with open(f"queries/q{query_no}.sql", "r") as f:
            query = f.read()

        query = f"PRAGMA disable_object_cache;\nPRAGMA threads={mp.cpu_count()};\n{query}" 


        latencies = 0

        for _ in range(range_size):
            process = mp.Process(target=run_query, args=(query, formats, query_no))
            processes.append(process)
    # print(processes)
    for process in processes:
        process.start()

    for process in processes:
        process.join()


    for process in processes:
        result = process.get()
        latencies += result['latency']
        data.append(result)

        average_latency = latencies / range_size
        bandwidth = result['bandwidth']

        print(f"Query {query_no} in {formats} format")
        print(f"overall_latency: {latencies}, average_latency: {average_latency}")
        print(f"bandwidth: {bandwidth} GiB/s")

    with open(f"results/current_results/bench_result.json", "w") as f:
        f.write(json.dumps(data))
    print("Benchmark finished")
