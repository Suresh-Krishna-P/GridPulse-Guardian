import time
import sys
import os
import requests
from datetime import datetime, timedelta

# Import pandas; fallback gracefully if it fails (although docker container will have it)
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import TickEvent, IngestionEvent
from pydantic import ValidationError

DATASET_DIR = "/app/sim datasets"

# Simple cache for weather API
weather_cache = {"timestamp": None, "data": None}

def fetch_live_weather_with_fallback(tick_idx: int, weather_df: pd.DataFrame):
    global weather_cache
    
    # Check cache (5 min expiry)
    if weather_cache["timestamp"] and datetime.now() < weather_cache["timestamp"] + timedelta(minutes=5):
        return weather_cache["data"]
        
    try:
        # Example coordinates (London)
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&current_weather=true", 
            timeout=5
        )
        resp.raise_for_status()
        data = resp.json().get("current_weather", {})
        
        weather_data = {
            "temperature": data.get("temperature", 20.0),
            "wind_speed_ms": data.get("windspeed", 5.0)
        }
        
        weather_cache["timestamp"] = datetime.now()
        weather_cache["data"] = weather_data
        print("Fetched live weather successfully.")
        return weather_data
        
    except Exception as e:
        print(f"Weather API failed: {e}. Falling back to dataset.")
        
        # Fallback to dataset using tick index (modulo row count to prevent out-of-bounds)
        if weather_df is not None and not weather_df.empty:
            row = weather_df.iloc[tick_idx % len(weather_df)]
            weather_data = {
                "temperature": float(row.get("temperature_c", 20.0)),
                "solar_irradiance": float(row.get("ghi_irradiance_wm2", 800.0)),
                "wind_speed_ms": float(row.get("wind_speed_ms", 5.0))
            }
        else:
            weather_data = {"temperature": 25.0, "solar_irradiance": 800.0, "wind_speed_ms": 5.0}
            
        weather_cache["timestamp"] = datetime.now()
        weather_cache["data"] = weather_data
        return weather_data

def load_datasets():
    weather_df = pd.DataFrame()
    demand_df = pd.DataFrame()
    
    if HAS_PANDAS and os.path.exists(DATASET_DIR):
        try:
            weather_path = os.path.join(DATASET_DIR, "weather.xlsx")
            demand_path = os.path.join(DATASET_DIR, "smart_meter_readings.xlsx")
            
            if os.path.exists(weather_path):
                weather_df = pd.read_excel(weather_path)
            if os.path.exists(demand_path):
                demand_df = pd.read_excel(demand_path)
            
            print(f"Loaded datasets. Weather: {len(weather_df)} rows. Demand: {len(demand_df)} rows.")
        except Exception as e:
            print(f"Error loading datasets: {e}")
    else:
        print("Pandas not installed or datasets dir missing. Running in mock mode.")
        
    return weather_df, demand_df

def main():
    redis_mgr = RedisManager()
    stream_in = "ticks_stream"
    stream_out = "ingestion_stream"
    group = "ingestion_group"
    consumer = "ingestion_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    
    weather_df, demand_df = load_datasets()
    print("Ingestion service started. Waiting for ticks...")
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                tick_event = TickEvent(**data)
                tick_idx = tick_event.tick - 1 # 0-indexed
                print(f"Received tick: {tick_event.tick}")
                
                # 1. Weather Data (Live with Fallback)
                weather_data = fetch_live_weather_with_fallback(tick_idx, weather_df)
                
                # 2. Grid Context Data (from dataset)
                base_load = 500.0
                if not demand_df.empty:
                    row = demand_df.iloc[tick_idx % len(demand_df)]
                    base_load = float(row.get("active_power_kw", 500.0))
                
                ingestion_event = IngestionEvent(
                    weather_data=weather_data,
                    grid_context={"base_load": base_load}
                )
                
                redis_mgr.publish(stream_out, ingestion_event.model_dump())
                print(f"Published IngestionEvent with trace_id {ingestion_event.trace_id}")
                redis_mgr.ack(stream_in, group, msg_id)
            except ValidationError as e:
                print(f"Validation Error: {e}")
                redis_mgr.ack(stream_in, group, msg_id)
            except Exception as e:
                print(f"Unexpected Error in Ingestion: {e}")
                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    main()
