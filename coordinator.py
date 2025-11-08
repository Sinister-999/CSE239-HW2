import rpyc
import sys
import os
import re
import time
import glob
import zipfile
import urllib.request
import gc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

WORKERS = [
    ("docker-worker1", 18861),
    ("docker-worker2", 18861),
    ("docker-worker3", 18861),
    ("docker-worker4", 18861),
    ("docker-worker5", 18861),
    ("docker-worker6", 18861),
    ("docker-worker7", 18861),
    ("docker-worker8", 18861),
]


def read_file_in_chunks(file_path, chunk_size=512*1024):  # Reduced to 512KB chunks
    """Generator that yields chunks of a file"""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    except FileNotFoundError:
        pass


def process_chunk(conn, chunk, chunk_id):
    """Process a single chunk on a worker - designed for concurrent execution"""
    try:
        print(f"Starting chunk {chunk_id} on worker")
        result = conn.root.map(chunk)
        # Convert remote object to local dictionary by iterating through it
        local_result = {}
        try:
            # Try to iterate through the remote dict
            for key in result:
                local_result[key] = result[key]
        except Exception as convert_error:
            print(f"Failed to convert remote result for chunk {chunk_id}: {convert_error}")
            # If conversion fails, return empty dict
            local_result = {}
        
        print(f"Completed chunk {chunk_id}")
        return local_result
    except Exception as e:
        print(f"Chunk {chunk_id} failed: {e}")
        return {}


def process_reduce_task(conn, group, task_id):
    """Process a single reduce task on a worker - designed for concurrent execution"""
    try:
        print(f"Starting reduce task {task_id}")
        result = conn.root.reduce(group)
        # Convert remote object to local dictionary by iterating through it
        local_result = {}
        try:
            for key in result:
                local_result[key] = result[key]
        except Exception as convert_error:
            print(f"Failed to convert remote result for reduce task {task_id}: {convert_error}")
            local_result = {}
        
        print(f"Completed reduce task {task_id}")
        return local_result
    except Exception as e:
        print(f"Reduce task {task_id} failed: {e}")
        return {}


def mapreduce_wordcount(input_files):
    # Wait for workers to start up and retry connection with exponential backoff
    connections = []
    max_retries = 5
    for host, port in WORKERS:
        connected = False
        for attempt in range(max_retries):
            try:
                print(f"Attempting to connect to {host}:{port} (attempt {attempt + 1})")
                conn = rpyc.connect(host, port, config={
                    "sync_request_timeout": 30,
                    "allow_pickle": True,
                    "allow_all_attrs": True
                })
                connections.append(conn)
                print(f"Successfully connected to {host}:{port}")
                connected = True
                break
            except Exception as e:
                print(f"Connection to {host}:{port} failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        
        if not connected:
            print(f"Failed to connect to {host}:{port} after {max_retries} attempts")
    
    if not connections:
        raise RuntimeError("No workers available")
    
    print(f"Connected to {len(connections)} workers")

    # MAP PHASE - Process files in chunks concurrently with controlled memory usage
    map_results = []
    chunk_count = 0
    
    print("Starting MAP phase with concurrent processing...")
    
    # Use a limited number of concurrent workers to prevent memory exhaustion
    max_concurrent_chunks = len(connections) * 2  # Allow 2x workers for better pipeline
    
    with ThreadPoolExecutor(max_workers=max_concurrent_chunks) as executor:
        futures = []
        
        # Process files one by one to control memory usage
        for file_path in input_files:
            print(f"Processing file: {file_path}")
            
            for chunk in read_file_in_chunks(file_path):
                if chunk.strip():  # Skip empty chunks
                    chunk_count += 1
                    
                    # Use round-robin to assign workers
                    conn = connections[(chunk_count - 1) % len(connections)]
                    
                    # Submit chunk for processing
                    future = executor.submit(process_chunk, conn, chunk, chunk_count)
                    futures.append(future)
                    
                    # Control memory by limiting concurrent tasks
                    if len(futures) >= max_concurrent_chunks:
                        # Process completed futures to free memory
                        completed_futures = []
                        for f in futures:
                            if f.done():
                                completed_futures.append(f)
                        
                        # Remove completed futures and collect results
                        for f in completed_futures:
                            try:
                                result = f.result()
                                map_results.append(result)
                            except Exception as e:
                                print(f"Chunk failed with exception: {e}")
                            futures.remove(f)
                            # Explicitly delete the future to free memory
                            del f
                        
                        # Clear the completed list
                        completed_futures.clear()
                        
                        # Periodic garbage collection
                        if chunk_count % 20 == 0:
                            print(f"Processed {chunk_count} chunks so far...")
                            gc.collect()
        
        # Wait for all remaining futures to complete
        print("Waiting for final chunks to complete...")
        for future in as_completed(futures):
            try:
                result = future.result()
                map_results.append(result)
            except Exception as e:
                print(f"Chunk failed with exception: {e}")
    
    # Force garbage collection after all chunks
    gc.collect()
    print(f"MAP phase completed - processed {len(map_results)} chunks successfully")

    # SHUFFLE PHASE
    print("Starting SHUFFLE phase...")
    grouped = {}
    for partial in map_results:
        for w, c in partial.items():
            if w in grouped:
                grouped[w].append(c)
            else:
                grouped[w] = [c]
    
    # Clear map_results to free memory
    map_results = None
    gc.collect()
    print("SHUFFLE phase completed")

    # REDUCE PHASE
    print("Starting REDUCE phase with concurrent processing...")
    partitions = partition_dict(grouped, len(connections))
    reduced_dicts = []
    
    # Process reduce tasks concurrently
    with ThreadPoolExecutor(max_workers=len(connections)) as executor:
        # Submit all reduce tasks
        future_to_task = {}
        
        for i, (conn, group) in enumerate(zip(connections, partitions)):
            future = executor.submit(process_reduce_task, conn, group, i + 1)
            future_to_task[future] = i + 1
        
        # Collect results as they complete
        for future in as_completed(future_to_task):
            task_id = future_to_task[future]
            try:
                result = future.result()
                reduced_dicts.append(result)
            except Exception as e:
                print(f"Reduce task {task_id} failed with exception: {e}")
                # Continue with other tasks

    # Clear partitions and grouped to free memory
    partitions = None
    grouped = None
    gc.collect()
    print("REDUCE phase completed")

    # FINAL AGGREGATION
    final_counts = {}
    for rd in reduced_dicts:
        for w, c in rd.items():
            if w in final_counts:
                final_counts[w] += c
            else:
                final_counts[w] = c

    # Close connections
    for conn in connections:
        try:
            conn.close()
        except:
            pass

    sorted_items = sorted(final_counts.items(), key=lambda x: (-x[1], x[0]))
    return sorted_items



def partition_dict(d, n):
    if n <= 0:
        n = 1
    buckets = [[] for _ in range(n)]
    for w, counts in d.items():
        idx = (hash(w) % n + n) % n
        buckets[idx].append((w, counts))
    return buckets


def download(url='https://mattmahoney.net/dc/enwik9.zip'):
    if isinstance(url, list) and url:
        url = url[0]
    if isinstance(url, list) and not url:
        url = 'https://mattmahoney.net/dc/enwik9.zip'

    os.makedirs("txt", exist_ok=True)
    zip_name = os.path.basename(url) or "enwik9.zip"
    zip_path = os.path.join(".", zip_name)

    if not os.path.exists(zip_path):
        try:
            urllib.request.urlretrieve(url, zip_path)
        except Exception as e:
            raise RuntimeError(f"Failed to download dataset: {e}")

    if not any(glob.glob("txt/*")):
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(".")
        except zipfile.BadZipFile:
            pass

        for f in glob.glob("*.txt"):
            dest = os.path.join("txt", os.path.basename(f))
            if not os.path.exists(dest):
                os.replace(f, dest)

    return ""


if __name__ == "__main__":
    print("Coordinator starting...")
    
    # Give workers time to start
    print("Waiting for workers to initialize...")
    time.sleep(10)  # Wait 10 seconds for workers to start
    
    text = download(sys.argv[1:])

    start_time = time.time()
    input_files = glob.glob('txt/*')
    print(f"Found {len(input_files)} input files")
    
    word_counts = mapreduce_wordcount(input_files)
    print('\nTOP 20 WORDS BY FREQUENCY\n')
    top20 = word_counts[0:20]
    longest = max(len(word) for word, count in top20) if top20 else 0
    i = 1
    for word, count in top20:
        print('%s.\t%-*s: %5s' % (i, longest + 1, word, count))
        i = i + 1

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Elapsed Time: {} seconds".format(elapsed_time))
