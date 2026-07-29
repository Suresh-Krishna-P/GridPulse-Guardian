import time
import sys
import os
import uuid
import threading
import json

# Force local connection since this script runs outside Docker
os.environ["REDIS_HOST"] = os.getenv("REDIS_HOST", "localhost")

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import DemandBid, SupplyOffer

def generate_load(redis_mgr, count):
    print(f"Injecting {count} trades...")
    start_time = time.time()
    
    for i in range(count):
        trace_id = str(uuid.uuid4())
        
        bid = DemandBid(
            trace_id=trace_id,
            quantity=50.0,
            price_limit=45.0,
            buyer_id=f"buyer_{i}"
        )
        redis_mgr.publish("demand_stream", bid.model_dump())
        
        offer = SupplyOffer(
            trace_id=trace_id,
            quantity=50.0,
            price_limit=35.0,
            seller_id=f"seller_{i}"
        )
        redis_mgr.publish("supply_stream", offer.model_dump())
        
    duration = time.time() - start_time
    print(f"Injection complete in {duration:.2f}s ({count/duration:.2f} trades/sec injected)")

def monitor_throughput(redis_mgr, target_count):
    stream_in = "safety_results"
    group = "load_test_group"
    consumer = "load_test_worker"
    
    redis_mgr.ensure_group(stream_in, group)
    
    print("Monitoring safety_results stream for throughput...")
    processed = 0
    start_time = time.time()
    
    while processed < target_count:
        messages = redis_mgr.r.xreadgroup(group, consumer, {stream_in: ">"}, count=100, block=1000)
        if messages:
            for stream_name, msg_list in messages:
                for msg_id, data in msg_list:
                    processed += 1
                    redis_mgr.ack(stream_in, group, msg_id)
                    
        if time.time() - start_time > 60:
            print(f"Timeout reached. Processed {processed}/{target_count}")
            break
            
    duration = time.time() - start_time
    print(f"Processed {processed} trades in {duration:.2f}s")
    print(f"System Throughput: {processed/duration:.2f} trades/sec")

def main():
    redis_mgr = RedisManager()
    target_count = 1000
    
    monitor_thread = threading.Thread(target=monitor_throughput, args=(redis_mgr, target_count))
    monitor_thread.daemon = True
    monitor_thread.start()
    
    # Wait for monitor to connect
    time.sleep(1)
    
    generate_load(redis_mgr, target_count)
    
    monitor_thread.join(timeout=65)

if __name__ == "__main__":
    main()
