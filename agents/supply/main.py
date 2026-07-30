import time
import sys
import os
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import IngestionEvent, SupplyOffer
from pydantic import ValidationError

def main():
    redis_mgr = RedisManager()
    stream_in = "ingestion_stream"
    stream_out = "supply_stream"
    group = "supply_group"
    consumer = "supply_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("Supply Agent started. Waiting for ingestion events...")
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                ingestion_event = IngestionEvent(**data)
                
                # Use actual solar irradiance from ingestion event
                irradiance = ingestion_event.weather_data.get('solar_irradiance', 0)
                # Ensure we handle missing values gracefully
                if irradiance is None: irradiance = 0
                
                quantity = max(1.0, (float(irradiance) / 1000) * 50)
                price_limit = random.uniform(20.0, 40.0)
                
                offer = SupplyOffer(
                    trace_id=ingestion_event.trace_id,
                    quantity=quantity,
                    price_limit=price_limit,
                    seller_id=f"solar_farm_{ingestion_event.trace_id[-4:]}",
                    carbon_intensity_gco2=0.0 # Clean energy!
                )
                
                redis_mgr.publish(stream_out, offer.model_dump())
                print(f"Published SupplyOffer for trace_id {offer.trace_id}")
                redis_mgr.ack(stream_in, group, msg_id)
            except ValidationError as e:
                print(f"Validation Error: {e}")
                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    main()
