import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import TickEvent, IngestionEvent
from pydantic import ValidationError

def main():
    redis_mgr = RedisManager()
    stream_in = "ticks_stream"
    stream_out = "ingestion_stream"
    group = "ingestion_group"
    consumer = "ingestion_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("Ingestion service started. Waiting for ticks...")
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                tick_event = TickEvent(**data)
                print(f"Received tick: {tick_event.tick}")
                
                # Mock weather/grid data
                ingestion_event = IngestionEvent(
                    weather_data={"temperature": 25.0, "solar_irradiance": 800},
                    grid_context={"base_load": 500}
                )
                
                redis_mgr.publish(stream_out, ingestion_event.model_dump())
                print(f"Published IngestionEvent with trace_id {ingestion_event.trace_id}")
                redis_mgr.ack(stream_in, group, msg_id)
            except ValidationError as e:
                print(f"Validation Error: {e}")
                # In real scenario, dead-letter here
                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    main()
