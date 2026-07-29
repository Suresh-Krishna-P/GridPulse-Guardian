import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import IngestionEvent, PricingSignal

import random

# Lightweight Q-Learning State
# State space: discretized temperature (low, med, high) x discretized load (low, med, high)
# Action space: price adjustments (decrease, hold, increase)
q_table = {}
epsilon = 0.1
alpha = 0.1
gamma = 0.9

def get_state(temp, load):
    t_state = "low" if temp < 15 else "med" if temp < 25 else "high"
    l_state = "low" if load < 300 else "med" if load < 700 else "high"
    return f"{t_state}_{l_state}"

def get_action(state):
    if state not in q_table:
        q_table[state] = {"decrease": 0, "hold": 0, "increase": 0}
        
    if random.random() < epsilon:
        return random.choice(["decrease", "hold", "increase"])
    
    return max(q_table[state], key=q_table[state].get)

def main():
    redis_mgr = RedisManager()
    stream_in = "ingestion_stream"
    stream_out = "pricing_signals"
    group = "pricing_group"
    consumer = "pricing_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("RL Pricing Agent started.")
    
    current_floor = 20.0
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                event = IngestionEvent(**data)
                
                temp = event.weather_data.get('temperature', 20)
                base_load = event.grid_context.get('base_load', 500)
                
                state = get_state(temp, base_load)
                action = get_action(state)
                
                if action == "increase": current_floor += 1.0
                elif action == "decrease": current_floor = max(10.0, current_floor - 1.0)
                
                ceiling = current_floor + 30.0
                
                # In a real RL loop, we would observe the reward (market liquidity) in the next step.
                # For this mock, we just update Q-table with a dummy reward based on action vs state logic.
                reward = 1.0 if (action == "increase" and state == "high_high") else 0.0
                
                # Q-learning update (simplified single step)
                if state not in q_table: q_table[state] = {"decrease": 0, "hold": 0, "increase": 0}
                q_table[state][action] = q_table[state][action] + alpha * (reward - q_table[state][action])
                
                signal = PricingSignal(
                    trace_id=event.trace_id,
                    recommended_floor=current_floor,
                    recommended_ceiling=ceiling
                )
                
                redis_mgr.publish(stream_out, signal.model_dump())
                
                # Mock Federated Learning broadcast - share local Q-table state
                redis_mgr.publish("federated_model_updates", {"agent_id": "pricing_1", "q_table": str(q_table)})
                
                print(f"Published RL PricingSignal: floor {current_floor}")
                redis_mgr.ack(stream_in, group, msg_id)
            except Exception as e:
                print(f"Pricing Error: {e}")
                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    main()
