import time
import sys
import os
import random

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import TradeCandidate, SafetyResult
from pydantic import ValidationError
import pandapower as pp

def create_base_network():
    net = pp.create_empty_network()
    # Create buses
    b0 = pp.create_bus(net, vn_kv=20., name="External Grid")
    b1 = pp.create_bus(net, vn_kv=20., name="Substation")
    b2 = pp.create_bus(net, vn_kv=20., name="Seller Bus")
    b3 = pp.create_bus(net, vn_kv=20., name="Buyer Bus")
    
    # Create external grid connection
    pp.create_ext_grid(net, bus=b0, vm_pu=1.0, name="Grid Connection")
    
    # Bottleneck line max_i_ka for ~50kW at 20kV
    # I = 0.05 MW / (sqrt(3) * 20 kV) = ~0.00144 kA
    pp.create_line_from_parameters(net, from_bus=b0, to_bus=b1, length_km=5.0, r_ohm_per_km=0.1, x_ohm_per_km=0.1, c_nf_per_km=10, max_i_ka=1.0, name="Line 0-1")
    pp.create_line_from_parameters(net, from_bus=b1, to_bus=b2, length_km=2.0, r_ohm_per_km=0.1, x_ohm_per_km=0.1, c_nf_per_km=10, max_i_ka=0.00144, name="Line 1-2 (Bottleneck)")
    pp.create_line_from_parameters(net, from_bus=b1, to_bus=b3, length_km=2.0, r_ohm_per_km=0.1, x_ohm_per_km=0.1, c_nf_per_km=10, max_i_ka=1.0, name="Line 1-3")
    
    # Base load (500 kW) on buyer bus
    pp.create_load(net, bus=b3, p_mw=0.5, q_mvar=0.1, name="Base Load")
    
    return net

def evaluate_trade_physics(net, candidate: TradeCandidate):
    import copy
    eval_net = copy.deepcopy(net)
    
    # Add trade participants to the grid
    # quantity is in kW, pandapower expects MW
    p_mw = candidate.quantity / 1000.0
    pp.create_sgen(eval_net, bus=2, p_mw=p_mw, name="Trade Seller")
    pp.create_load(eval_net, bus=3, p_mw=p_mw, name="Trade Buyer")
    
    try:
        pp.runpp(eval_net)
        max_loading = eval_net.res_line.loading_percent.max()
        if max_loading > 100.0:
            overloaded_line = eval_net.line.name[eval_net.res_line.loading_percent.idxmax()]
            return False, f"Thermal limit exceeded: {max_loading:.1f}% on {overloaded_line}"
        return True, None
    except Exception as e:
        return False, f"Grid unstable (Power flow failed to converge)"

def main():
    redis_mgr = RedisManager()
    stream_in = "trade_candidates"
    stream_overrides = "human_overrides"
    stream_out = "safety_results"
    group = "safety_group"
    consumer = "safety_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    redis_mgr.ensure_group(stream_overrides, group)
    print("Safety Agent started. Waiting for trade candidates and overrides...")
    
    pending_trades = {}
    
    # Build persistent base grid model
    base_net = create_base_network()
    print("Base physical grid model instantiated.")
    
    while True:
        streams = {stream_in: ">", stream_overrides: ">"}
        messages = redis_mgr.r.xreadgroup(group, consumer, streams, count=10, block=100)
        
        if messages:
            for stream_name, msg_list in messages:
                for msg_id, data in msg_list:
                    try:
                        # Deserialize
                        import json
                        parsed = {}
                        for k, v in data.items():
                            try:
                                parsed[k] = json.loads(v)
                            except:
                                parsed[k] = v
                                
                        if stream_name == stream_in:
                            candidate = TradeCandidate(**parsed)
                            status = "approved"
                            reason = None
                            
                            # HITL Threshold check
                            if candidate.price > 45.0 or candidate.quantity > 45.0:
                                status = "pending_review"
                                reason = "Anomalous price/quantity. Awaiting manual override."
                                pending_trades[candidate.trace_id] = candidate
                            else:
                                # Physics Evaluation using Pandapower
                                is_safe, reject_reason = evaluate_trade_physics(base_net, candidate)
                                if not is_safe:
                                    status = "rejected"
                                    reason = reject_reason
                            
                            result = SafetyResult(
                                trace_id=candidate.trace_id,
                                trade_candidate=candidate,
                                status=status,
                                reason=reason
                            )
                            redis_mgr.publish(stream_out, result.model_dump())
                            
                        elif stream_name == stream_overrides:
                            trace_id = parsed.get("trace_id")
                            decision = parsed.get("decision")
                            if trace_id in pending_trades:
                                candidate = pending_trades.pop(trace_id)
                                result = SafetyResult(
                                    trace_id=trace_id,
                                    trade_candidate=candidate,
                                    status=decision,
                                    reason=f"Human overridden: {decision}"
                                )
                                redis_mgr.publish(stream_out, result.model_dump())
                                print(f"Processed HITL override for {trace_id}: {decision}")
                                
                        redis_mgr.ack(stream_name, group, msg_id)
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
