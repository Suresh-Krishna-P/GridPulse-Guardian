from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
import uuid

def generate_trace_id():
    return str(uuid.uuid4())

class BaseEvent(BaseModel):
    trace_id: str = Field(default_factory=generate_trace_id)
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

class TickEvent(BaseEvent):
    tick: int

class IngestionEvent(BaseEvent):
    weather_data: Dict[str, Any]
    grid_context: Dict[str, Any]

class DemandBid(BaseEvent):
    quantity: float
    price_limit: float
    buyer_id: str

class SupplyOffer(BaseEvent):
    quantity: float
    price_limit: float
    seller_id: str

class TradeCandidate(BaseEvent):
    buyer_id: str
    seller_id: str
    quantity: float
    price: float

class SafetyResult(BaseEvent):
    trade_candidate: TradeCandidate
    status: str # 'approved' or 'rejected'
    reason: Optional[str] = None
