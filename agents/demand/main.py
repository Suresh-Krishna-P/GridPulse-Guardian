import time
import sys
import os
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import IngestionEvent, DemandBid
from pydantic import ValidationError

def main():
    redis_mgr = RedisManager()
    stream_in = "ingestion_stream"
    stream_out = "demand_stream"
    group = "demand_group"
    consumer = "demand_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("Demand Agent started. Waiting for ingestion events...")
    
    # Background thread for heartbeats and soft-kill commands
    import threading
    import json
    
    def background_tasks():
        last_id = "$"
        while True:
            # Emit heartbeat
            redis_mgr.publish("heartbeats", {"agent": "demand"})
            
            # Check for soft-kill command
            try:
                msgs = redis_mgr.r.xread({"control_commands": last_id}, count=10, block=1000)
                if msgs:
                    for stream, msg_list in msgs:
                        for msg_id, data in msg_list:
                            last_id = msg_id
                            if data.get('agent') == 'demand' and data.get('command') == 'restart':
                                print("Received soft-kill command from supervisor. Exiting!")
                                os._exit(1)
            except Exception as e:
                pass
            time.sleep(4) # heartbeat every 5s total loop
            
    threading.Thread(target=background_tasks, daemon=True).start()
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                ingestion_event = IngestionEvent(**data)
                
                # Use the actual base_load from the ingestion event
                base_load = ingestion_event.grid_context.get('base_load', 500)
                temp = ingestion_event.weather_data.get('temperature', 20)
                
                # Mock demand based on actual base_load
                quantity = max(0.1, float(base_load) + (30 - temp) * 2)
                price_limit = random.uniform(30.0, 40.0)
                
                # 20% chance to trigger an anomalous high price for HITL review
                if random.random() < 0.2:
                    price_limit = random.uniform(46.0, 60.0)
                
                bid = DemandBid(
                    trace_id=ingestion_event.trace_id,
                    quantity=quantity,
                    price_limit=price_limit,
                    buyer_id="buyer_1"
                )
                
                redis_mgr.publish(stream_out, bid.model_dump())
                print(f"Published DemandBid for trace_id {bid.trace_id}")
                redis_mgr.ack(stream_in, group, msg_id)
            except ValidationError as e:
                print(f"Validation Error: {e}")
                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    main()
