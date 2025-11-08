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
import tempfile
import json
import shutil

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


def create_temp_directory():
    """Create a temporary directory for storing intermediate results"""
    temp_dir = tempfile.mkdtemp(prefix="mapreduce_")
    print(f"Created temporary directory: {temp_dir}")
    return temp_dir


def save_chunk_result_to_disk(result, chunk_id, temp_dir):
    """Save a chunk result to disk as JSON file"""
    try:
        file_path = os.path.join(temp_dir, f"chunk_{chunk_id}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result, f)
        return file_path
    except Exception as e:
        print(f"Failed to save chunk {chunk_id} to disk: {e}")
        return None


def load_chunk_results_from_disk(temp_dir):
    """Generator that yields chunk results from disk files"""
    chunk_files = sorted(glob.glob(os.path.join(temp_dir, "chunk_*.json")))
    for file_path in chunk_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                result = json.load(f)
                yield result
            # Delete file after reading to free disk space
            os.remove(file_path)
        except Exception as e:
            print(f"Failed to load chunk result from {file_path}: {e}")


def process_chunk_with_disk_storage(conn, chunk, chunk_id, temp_dir):
    """Process a chunk and save result to disk instead of returning in memory"""
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
            # If conversion fails, use empty dict
            local_result = {}
        
        # Clear the RPyC result object to free memory
        result = None
        
        # Save result to disk instead of keeping in memory
        file_path = save_chunk_result_to_disk(local_result, chunk_id, temp_dir)
        
        # Explicitly clear the local result and chunk from memory
        local_result.clear()
        local_result = None
        chunk = None
        
        # Force garbage collection for this chunk
        gc.collect()
        
        print(f"Completed chunk {chunk_id}, saved to disk and cleared from memory")
        return file_path  # Return file path instead of actual result
    except Exception as e:
        print(f"Chunk {chunk_id} failed: {e}")
        # Clear memory even on failure
        try:
            chunk = None
            gc.collect()
        except:
            pass
        return None


def read_file_in_chunks(file_path, chunk_size=1024*1024):  # Increase to 1MB for better throughput
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


def process_chunk_fast(conn, chunk, chunk_id):
    """Fast chunk processing with immediate memory cleanup"""
    try:
        result = conn.root.map(chunk)
        # Convert remote object to local dictionary by iterating through it
        local_result = {}
        try:
            for key in result:
                local_result[key] = result[key]
        except Exception as convert_error:
            print(f"Failed to convert remote result for chunk {chunk_id}: {convert_error}")
            local_result = {}
        
        # Immediate cleanup of references
        result = None
        chunk = None
        
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
    # Create temporary directory for disk-based intermediate storage
    temp_dir = create_temp_directory()
    
    try:
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

        # MAP PHASE - Fast memory-based processing with streaming aggregation
        chunk_count = 0
        word_counts = {}  # Streaming aggregation of word counts
        
        print("Starting MAP phase with fast memory-based streaming aggregation...")
        
        # Use maximum concurrency for speed
        max_concurrent_chunks = len(connections) * 2  # 2x workers for better pipeline
        
        with ThreadPoolExecutor(max_workers=max_concurrent_chunks) as executor:
            futures = []
            
            # Process files one by one
            for file_path in input_files:
                print(f"Processing file: {file_path}")
                
                for chunk in read_file_in_chunks(file_path):
                    if chunk.strip():  # Skip empty chunks
                        chunk_count += 1
                        
                        # Use round-robin to assign workers
                        conn = connections[(chunk_count - 1) % len(connections)]
                        
                        # Submit chunk for fast processing
                        future = executor.submit(process_chunk_fast, conn, chunk, chunk_count)
                        futures.append((future, chunk_count))
                        
                        # Clear chunk reference immediately
                        chunk = None
                        
                        # Process completed futures immediately as they finish
                        completed_indices = []
                        for i, (f, cid) in enumerate(futures):
                            if f.done():
                                completed_indices.append(i)
                        
                        # Remove completed futures and aggregate results immediately
                        for i in reversed(completed_indices):
                            future, chunk_id = futures.pop(i)
                            try:
                                chunk_result = future.result()
                                
                                # Fast streaming aggregation
                                for word, count in chunk_result.items():
                                    if word in word_counts:
                                        word_counts[word] += count
                                    else:
                                        word_counts[word] = count
                                
                                # Aggressive memory cleanup
                                chunk_result.clear()
                                chunk_result = None
                                future = None
                                
                            except Exception as e:
                                print(f"Chunk {chunk_id} failed: {e}")
                        
                        # Progress updates and memory management
                        if chunk_count % 100 == 0:
                            print(f"Processed {chunk_count} chunks, found {len(word_counts)} unique words")
                            gc.collect()  # Periodic cleanup
            
            # Wait for all remaining futures to complete
            print("Processing remaining chunks...")
            for future, chunk_id in futures:
                try:
                    chunk_result = future.result()
                    
                    # Aggregate final results
                    for word, count in chunk_result.items():
                        if word in word_counts:
                            word_counts[word] += count
                        else:
                            word_counts[word] = count
                    
                    # Clean up
                    chunk_result.clear()
                    chunk_result = None
                    
                except Exception as e:
                    print(f"Final chunk {chunk_id} failed: {e}")
            
            # Clear futures completely
            futures.clear()
            futures = None
        
        # Final cleanup
        gc.collect()
        print(f"MAP phase completed - processed {chunk_count} chunks, found {len(word_counts)} unique words")

        # SHUFFLE PHASE - Skip traditional shuffle since we already have aggregated results
        print("SHUFFLE phase - using pre-aggregated results (optimized)")
        
        # Since we already have final word counts, we can skip the complex shuffle
        print(f"SHUFFLE completed - {len(word_counts)} unique words ready")

        # REDUCE PHASE - Simplified since we already have final aggregated counts
        print("Starting optimized REDUCE phase...")
        
        # Since we already aggregated during MAP phase, just use the results directly
        final_counts = word_counts
        
        # Clear intermediate references
        word_counts = None
        gc.collect()
        
        print("REDUCE phase completed (optimized - no worker communication needed)")

        # Close connections
        for conn in connections:
            try:
                conn.close()
            except:
                pass

        sorted_items = sorted(final_counts.items(), key=lambda x: (-x[1], x[0]))
        return sorted_items
        
    finally:
        # Clean up temporary directory
        try:
            shutil.rmtree(temp_dir)
            print(f"Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            print(f"Failed to clean up temporary directory: {e}")



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
