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
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                ingestion_event = IngestionEvent(**data)
                
                # Use the actual base_load from the ingestion event
                base_load = ingestion_event.grid_context.get('base_load', 500)
                temp = ingestion_event.weather_data.get('temperature', 20)
                
                # Mock demand based on actual base_load
                quantity = float(base_load) + (30 - temp) * 2
                price_limit = random.uniform(30.0, 50.0)
                
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
