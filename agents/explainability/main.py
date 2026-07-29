import sys
import os
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import SafetyResult, ExplanationEvent

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://host.docker.internal:11434/api/generate")

def generate_explanation(result: SafetyResult):
    prompt = f"The Grid Safety Auditor {result.status} a trade for {result.trade_candidate.quantity} kW at ${result.trade_candidate.price} between {result.trade_candidate.buyer_id} and {result.trade_candidate.seller_id}."
    if result.reason:
        prompt += f" The reason given was: {result.reason}."
    prompt += " Please write a one-sentence plain English explanation of this decision for a dashboard."
    
    try:
        response = requests.post(OLLAMA_URL, json={
            "model": "llama3.2:3b",
            "prompt": prompt,
            "stream": False
        }, timeout=10)
        response.raise_for_status()
        return response.json().get("response", "Explanation generated locally.")
    except Exception as e:
        print(f"Ollama call failed: {e}")
        # Deterministic fallback
        return f"Trade was {result.status}. {result.reason if result.reason else 'Grid conditions are stable.'}"

def main():
    redis_mgr = RedisManager()
    stream_in = "safety_results"
    stream_out = "explanations"
    group = "explain_group"
    consumer = "explain_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("Explainability Agent started. Waiting for safety results...")
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                result = SafetyResult(**data)
                explanation_text = generate_explanation(result)
                
                explanation = ExplanationEvent(
                    trace_id=result.trace_id,
                    trade_id=result.trace_id,
                    decision=result.status,
                    explanation=explanation_text
                )
                
                redis_mgr.publish(stream_out, explanation.model_dump())
                print(f"Published Explanation for {result.trace_id}")
                redis_mgr.ack(stream_in, group, msg_id)
            except Exception as e:
                print(f"Explainability Error: {e}")
                redis_mgr.ack(stream_in, group, msg_id)

if __name__ == "__main__":
    main()
