import rpyc
import re
import socket

class MapReduceService(rpyc.Service):
    def exposed_map(self, text_chunk):
        worker_id = socket.gethostname()
        print(f"Worker {worker_id} starting map task with {len(text_chunk)} characters")
        
        tokens = re.findall(r"[A-Za-z]+", text_chunk.lower())
        counts = {}
        for t in tokens:
            if t in counts:
                counts[t] += 1
            else:
                counts[t] = 1
        
        print(f"Worker {worker_id} completed map task, found {len(counts)} unique words")
        return counts

    def exposed_reduce(self, grouped_items):
        worker_id = socket.gethostname()
        print(f"Worker {worker_id} starting reduce task with {len(grouped_items)} items")
        
        out = {}
        for w, values in grouped_items:
            total = 0
            for v in values:
                total += int(v)
            out[w] = total
        
        print(f"Worker {worker_id} completed reduce task, processed {len(out)} words")
        return out

if __name__ == "__main__":
    worker_id = socket.gethostname()
    print(f"Starting worker {worker_id} on port 18861")
    
    from rpyc.utils.server import ThreadedServer
    try:
        t = ThreadedServer(MapReduceService, port=18861, auto_register=False)
        print(f"Worker {worker_id} ready to accept connections")
        t.start()
    except Exception as e:
        print(f"Worker {worker_id} failed to start: {e}")
        raise
