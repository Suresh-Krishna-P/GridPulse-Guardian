import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import DemandBid, SupplyOffer, TradeCandidate
from pydantic import ValidationError

def main():
    redis_mgr = RedisManager()
    stream_demand = "demand_stream"
    stream_supply = "supply_stream"
    stream_out = "trade_candidates"
    group = "market_group"
    consumer = "market_worker_1"
    
    redis_mgr.ensure_group(stream_demand, group)
    redis_mgr.ensure_group(stream_supply, group)
    
    # Check for active healing session
    healing_session_id = redis_mgr.r.get("healing_state:market")
    if healing_session_id:
        print(f"Market Agent booting in HEALING WINDOW for session {healing_session_id}")
        
    print("Market Agent started. Waiting for bids and offers...")
    
    # Background thread for heartbeats and soft-kill commands
    import threading
    import json
    
    def background_tasks():
        last_id = "$"
        while True:
            # Emit heartbeat
            redis_mgr.publish("heartbeats", {"agent": "market"})
            
            # Check for soft-kill command
            try:
                msgs = redis_mgr.r.xread({"control_commands": last_id}, count=10, block=1000)
                if msgs:
                    for stream, msg_list in msgs:
                        for msg_id, data in msg_list:
                            last_id = msg_id
                            if data.get('agent') == 'market' and data.get('command') == 'restart':
                                print("Received soft-kill command from supervisor. Exiting!")
                                os._exit(1)
            except Exception as e:
                pass
            time.sleep(4)
            
    threading.Thread(target=background_tasks, daemon=True).start()
    
    pending_bids = {}
    pending_offers = {}
    
    while True:
        # Read from both streams
        messages = redis_mgr.r.xreadgroup(group, consumer, {stream_demand: ">", stream_supply: ">"}, count=10, block=1000)
        
        if messages:
            for stream_name, msg_list in messages:
                for msg_id, data in msg_list:
                    # Deserialize
                    import json
                    parsed = {}
                    for k, v in data.items():
                        try:
                            parsed[k] = json.loads(v)
                        except json.JSONDecodeError:
                            parsed[k] = v
                            
                    try:
                        if stream_name == stream_demand:
                            bid = DemandBid(**parsed)
                            pending_bids[bid.trace_id] = (msg_id, bid)
                        elif stream_name == stream_supply:
                            offer = SupplyOffer(**parsed)
                            pending_offers[offer.trace_id] = (msg_id, offer)
                    except ValidationError as e:
                        print(f"Validation Error in Market: {e}")
                        redis_mgr.ack(stream_name, group, msg_id)
        
        # Match if both exist for a trace_id
        matched_traces = []
        for trace_id in list(pending_bids.keys()):
            if trace_id in pending_offers:
                bid_msg_id, bid = pending_bids[trace_id]
                offer_msg_id, offer = pending_offers[trace_id]
                
                # Carbon Aware Optimization
                if offer.carbon_intensity_gco2 > 1000:
                    print(f"Rejected trace_id {trace_id} due to high carbon intensity: {offer.carbon_intensity_gco2}")
                    # Acknowledge but do not emit TradeCandidate
                    redis_mgr.ack(stream_demand, group, bid_msg_id)
                    redis_mgr.ack(stream_supply, group, offer_msg_id)
                    matched_traces.append(trace_id)
                    continue
                
                # Penalty for moderate carbon
                carbon_penalty = 0
                if offer.carbon_intensity_gco2 > 500:
                    carbon_penalty = 5.0
                
                # Create TradeCandidate
                candidate = TradeCandidate(
                    trace_id=trace_id,
                    buyer_id=bid.buyer_id,
                    seller_id=offer.seller_id,
                    quantity=min(bid.quantity, offer.quantity),
                    price=((bid.price_limit + offer.price_limit) / 2) + carbon_penalty,
                    healing_session_id=healing_session_id
                )
                
                redis_mgr.publish(stream_out, candidate.model_dump())
                print(f"Published TradeCandidate for trace_id {trace_id}")
                
                redis_mgr.ack(stream_demand, group, bid_msg_id)
                redis_mgr.ack(stream_supply, group, offer_msg_id)
                
                matched_traces.append(trace_id)
                
        for trace_id in matched_traces:
            del pending_bids[trace_id]
            del pending_offers[trace_id]

if __name__ == "__main__":
    main()
