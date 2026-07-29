import asyncio
import json
import os
import sys
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_mgr = RedisManager()
stream_in = "safety_results"
group = "api_group"
consumer = "api_worker_1"

# We just want to consume latest, but to make it simple and reliable for multiple UI clients,
# we can use a small redis pub/sub or just broadcast. Since we're reading from stream,
# we read, ack, and put in an asyncio Queue to broadcast to SSE clients.

clients = set()

async def read_redis_stream():
    redis_mgr.ensure_group(stream_in, group)
    print("API started reading safety_results stream...")
    while True:
        # Read from Redis (blocking in thread is bad for async, so we'll use a short timeout and asyncio.sleep)
        messages = redis_mgr.r.xreadgroup(group, consumer, {stream_in: ">"}, count=10, block=100)
        if messages:
            for stream_name, msg_list in messages:
                for msg_id, data in msg_list:
                    parsed = {}
                    for k, v in data.items():
                        try:
                            parsed[k] = json.loads(v)
                        except:
                            parsed[k] = v
                    
                    # Broadcast to clients
                    for queue in list(clients):
                        await queue.put(parsed)
                        
                    redis_mgr.ack(stream_in, group, msg_id)
        
        await asyncio.sleep(0.1)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(read_redis_stream())

async def sse_generator(request: Request, queue: asyncio.Queue):
    try:
        while True:
            if await request.is_disconnected():
                break
            data = await queue.get()
            yield f"data: {json.dumps(data)}\n\n"
    except asyncio.CancelledError:
        pass
    finally:
        clients.remove(queue)

@app.get("/stream")
async def stream_events(request: Request):
    queue = asyncio.Queue()
    clients.add(queue)
    return StreamingResponse(sse_generator(request, queue), media_type="text/event-stream")

@app.get("/health")
def health():
    return {"status": "ok"}
