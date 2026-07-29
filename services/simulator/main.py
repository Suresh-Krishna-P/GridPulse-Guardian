import time
import sys
import os

# Add parent dir to path to import shared
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import TickEvent

def main():
    redis_mgr = RedisManager()
    tick_count = 0
    print("Simulator started. Emitting ticks...")
    while True:
        tick_count += 1
        event = TickEvent(tick=tick_count)
        redis_mgr.publish("ticks_stream", event.model_dump())
        print(f"Emitted tick {tick_count}")
        time.sleep(5)  # Demo speed control

if __name__ == "__main__":
    main()
