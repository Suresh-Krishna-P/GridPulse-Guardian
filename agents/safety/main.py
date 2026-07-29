import time
import sys
import os
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import TradeCandidate, SafetyResult
from pydantic import ValidationError

def main():
    redis_mgr = RedisManager()
    stream_in = "trade_candidates"
    stream_out = "safety_results"
    group = "safety_group"
    consumer = "safety_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("Safety Agent started. Waiting for trade candidates...")
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                candidate = TradeCandidate(**data)
                
                # Mock Pandapower simulation logic
                # Randomly fail 10% of trades to show rejection in UI
                status = "approved"
                reason = None
                
                if random.random() < 0.1:
                    status = "rejected"
                    reason = "Line overload in mock grid"
                
                result = SafetyResult(
                    trace_id=candidate.trace_id,
                    trade_candidate=candidate,
                    status=status,
                    reason=reason
                )
                
                redis_mgr.publish(stream_out, result.model_dump())
                print(f"Published SafetyResult for trace_id {result.trace_id}: {status}")
                redis_mgr.ack(stream_in, group, msg_id)
            except Exception as e:
                # FAIL-CLOSED principle
                print(f"Safety Check Exception (failing closed): {e}")
                
                if 'trace_id' in data:
                    try:
                        # Attempt to reject with the trace id if we have one
                        trace_id = json.loads(data['trace_id']) if isinstance(data['trace_id'], str) else data['trace_id']
                        # Mock the candidate back based on what we can parse
                        result = SafetyResult(
                            trace_id=trace_id,
                            trade_candidate=TradeCandidate(**data), # Might fail
                            status="rejected",
                            reason=f"System exception: {str(e)}"
                        )
                    except:
                         result = SafetyResult(
                            trace_id="unknown",
                            trade_candidate=TradeCandidate(trace_id="unknown", buyer_id="unknown", seller_id="unknown", quantity=0.0, price=0.0),
                            status="rejected",
                            reason=f"Unparseable input. Exception: {str(e)}"
                        )
                    
                    redis_mgr.publish(stream_out, result.model_dump())

                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    import json
    main()
