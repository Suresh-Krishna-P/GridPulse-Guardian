import redis
import json
import os
import time

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

class RedisManager:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.wait_for_redis()

    def wait_for_redis(self):
        while True:
            try:
                self.r.ping()
                print("Connected to Redis")
                break
            except redis.exceptions.ConnectionError:
                print("Waiting for Redis...")
                time.sleep(1)

    def ensure_group(self, stream, group):
        try:
            self.r.xgroup_create(stream, group, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def publish(self, stream: str, data: dict):
        # Convert all values to strings for Redis stream compatibility
        flat_data = {k: json.dumps(v) if not isinstance(v, str) else v for k, v in data.items()}
        self.r.xadd(stream, flat_data)

    def consume(self, stream: str, group: str, consumer: str, count=1, block=5000):
        # Read from group
        messages = self.r.xreadgroup(group, consumer, {stream: ">"}, count=count, block=block)
        parsed_messages = []
        if messages:
            for stream_name, msg_list in messages:
                for msg_id, data in msg_list:
                    # Reconstruct dict
                    parsed = {}
                    for k, v in data.items():
                        try:
                            parsed[k] = json.loads(v)
                        except json.JSONDecodeError:
                            parsed[k] = v
                    parsed_messages.append((msg_id, parsed))
        return parsed_messages

    def ack(self, stream: str, group: str, msg_id: str):
        self.r.xack(stream, group, msg_id)
