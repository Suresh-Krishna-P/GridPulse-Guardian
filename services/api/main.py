import asyncio
import json
import os
import sys
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.checkpoint.manager import Session, AgentCheckpoint, HealingEvent

app = FastAPI()

# Add prometheus asgi middleware to route /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_mgr = RedisManager()
stream_safety = "safety_results"
stream_pricing = "pricing_signals"
stream_explain = "explanations"
stream_federated = "federated_model_updates"
group = "api_group"
consumer = "api_worker_1"

clients = set()

async def read_redis_stream():
    for s in [stream_safety, stream_pricing, stream_explain, stream_federated]:
        redis_mgr.ensure_group(s, group)
        
    print("API started reading multiple streams...")
    while True:
        streams = {
            stream_safety: ">",
            stream_pricing: ">",
            stream_explain: ">",
            stream_federated: ">"
        }
        messages = redis_mgr.r.xreadgroup(group, consumer, streams, count=10, block=100)
        
        if messages:
            for stream_name, msg_list in messages:
                for msg_id, data in msg_list:
                    parsed = {}
                    for k, v in data.items():
                        try:
                            parsed[k] = json.loads(v)
                        except:
                            parsed[k] = v
                    
                    # Inject type for frontend multiplexing
                    parsed["_type"] = stream_name
                    
                    for queue in list(clients):
                        await queue.put(parsed)
                        
                    redis_mgr.ack(stream_name, group, msg_id)
        
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

from pydantic import BaseModel

class OverrideRequest(BaseModel):
    trace_id: str
    decision: str # "approved" or "rejected"

@app.post("/override")
async def handle_override(req: OverrideRequest):
    redis_mgr.publish("human_overrides", req.model_dump())
    return {"status": "ok", "message": f"Published override for {req.trace_id}"}

@app.get("/health")
def health():
    return {"status": "ok"}

class HealApprovalRequest(BaseModel):
    session_id: str
    decision: str

@app.post("/heal_approval")
async def handle_heal_approval(req: HealApprovalRequest):
    redis_mgr.publish("human_approvals", req.model_dump())
    return {"status": "ok", "message": f"Published {req.decision} for session {req.session_id}"}

@app.get("/healing_sessions")
def get_healing_sessions():
    session = Session()
    try:
        checkpoints = session.query(AgentCheckpoint).order_by(AgentCheckpoint.created_at.desc()).limit(5).all()
        res = []
        for ckpt in checkpoints:
            events = session.query(HealingEvent).filter_by(healing_session_id=ckpt.healing_session_id).order_by(HealingEvent.timestamp.asc()).all()
            res.append({
                "checkpoint_id": ckpt.checkpoint_id,
                "session_id": ckpt.healing_session_id,
                "agent_name": ckpt.agent_name,
                "trigger_reason": ckpt.trigger_reason,
                "created_at": ckpt.created_at.isoformat(),
                "status": ckpt.status,
                "events": [{"step": e.step, "timestamp": e.timestamp.isoformat(), "detail": e.detail} for e in events]
            })
        return res
    finally:
        session.close()
