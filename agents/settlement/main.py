import os
import sys
import hashlib
import time
from sqlalchemy import create_engine, Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.schemas.events import SafetyResult, SettlementEvent

DB_USER = os.getenv("DB_USER", "gridpulse")
DB_PASS = os.getenv("DB_PASS", "gridpulse")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "gridpulse")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
Base = declarative_base()

class LedgerEntry(Base):
    __tablename__ = 'ledger'
    id = Column(Integer, primary_key=True)
    trade_id = Column(String, unique=True, nullable=False)
    amount = Column(Float, nullable=False)
    buyer_id = Column(String, nullable=False)
    seller_id = Column(String, nullable=False)
    previous_hash = Column(String, nullable=False)
    current_hash = Column(String, nullable=False)
    healing_session_id = Column(String, nullable=True)

def init_db():
    while True:
        try:
            Base.metadata.create_all(engine)
            print("Connected to Postgres and initialized tables")
            break
        except Exception as e:
            print(f"Waiting for Postgres... {e}")
            time.sleep(2)

def main():
    init_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    redis_mgr = RedisManager()
    stream_in = "safety_results"
    stream_out = "settlement_events"
    group = "settlement_group"
    consumer = "settlement_worker_1"
    
    redis_mgr.ensure_group(stream_in, group)
    print("Settlement Agent started. Waiting for safety results...")
    
    import threading
    import uuid
    
    def process_rollbacks():
        local_redis = RedisManager()
        last_id = "$"
        while True:
            try:
                msgs = local_redis.r.xread({"rollbacks": last_id}, count=10, block=1000)
                if msgs:
                    for s, msg_list in msgs:
                        for msg_id, data in msg_list:
                            last_id = msg_id
                            session_id = data.get(b'session_id', data.get('session_id', b'')).decode()
                            
                            local_session = Session()
                            # Find all trades for this session
                            trades = local_session.query(LedgerEntry).filter_by(healing_session_id=session_id).all()
                            for trade in trades:
                                if "_REVERSAL" in trade.trade_id: continue
                                
                                # Reversal logic
                                rev_trade_id = f"{trade.trade_id}_REVERSAL"
                                # check idempotency
                                if local_session.query(LedgerEntry).filter_by(trade_id=rev_trade_id).first():
                                    continue
                                    
                                last_entry = local_session.query(LedgerEntry).order_by(LedgerEntry.id.desc()).first()
                                prev_hash = last_entry.current_hash if last_entry else "0" * 64
                                
                                amount = -trade.amount
                                data_string = f"{rev_trade_id}{amount}{trade.buyer_id}{trade.seller_id}{prev_hash}"
                                curr_hash = hashlib.sha256(data_string.encode()).hexdigest()
                                
                                rev_entry = LedgerEntry(
                                    trade_id=rev_trade_id,
                                    amount=amount,
                                    buyer_id=trade.buyer_id,
                                    seller_id=trade.seller_id,
                                    previous_hash=prev_hash,
                                    current_hash=curr_hash,
                                    healing_session_id=session_id
                                )
                                local_session.add(rev_entry)
                                local_session.commit()
                                print(f"Settlement: Reverted trade {trade.trade_id} with hash {curr_hash[:8]} (Rollback {session_id})")
                            local_session.close()
            except Exception as e:
                print(f"Rollback thread error: {e}")
            time.sleep(1)
            
    threading.Thread(target=process_rollbacks, daemon=True).start()
    
    while True:
        messages = redis_mgr.consume(stream_in, group, consumer)
        for msg_id, data in messages:
            try:
                result = SafetyResult(**data)
                
                if result.status == "approved":
                    trade = result.trade_candidate
                    trade_id = result.trace_id # use trace_id as trade_id for simplicity
                    
                    # Idempotency check
                    existing = session.query(LedgerEntry).filter_by(trade_id=trade_id).first()
                    if existing:
                        print(f"Trade {trade_id} already settled. Skipping.")
                        redis_mgr.ack(stream_in, group, msg_id)
                        continue
                    
                    # Hash chain
                    last_entry = session.query(LedgerEntry).order_by(LedgerEntry.id.desc()).first()
                    prev_hash = last_entry.current_hash if last_entry else "0" * 64
                    
                    amount = trade.quantity * trade.price
                    data_string = f"{trade_id}{amount}{trade.buyer_id}{trade.seller_id}{prev_hash}"
                    curr_hash = hashlib.sha256(data_string.encode()).hexdigest()
                    
                    new_entry = LedgerEntry(
                        trade_id=trade_id,
                        amount=amount,
                        buyer_id=trade.buyer_id,
                        seller_id=trade.seller_id,
                        previous_hash=prev_hash,
                        current_hash=curr_hash,
                        healing_session_id=trade.healing_session_id
                    )
                    
                    session.add(new_entry)
                    session.commit()
                    
                    settlement = SettlementEvent(
                        trace_id=result.trace_id,
                        trade_id=trade_id,
                        amount=amount,
                        buyer_id=trade.buyer_id,
                        seller_id=trade.seller_id,
                        previous_hash=prev_hash,
                        current_hash=curr_hash
                    )
                    redis_mgr.publish(stream_out, settlement.model_dump())
                    print(f"Settled trade {trade_id} with hash {curr_hash[:8]}...")
                
                redis_mgr.ack(stream_in, group, msg_id)
            except IntegrityError:
                session.rollback()
                print("Concurrency issue on insert. Rolled back.")
                redis_mgr.ack(stream_in, group, msg_id)
            except Exception as e:
                print(f"Settlement Error: {e}")
                session.rollback()

if __name__ == "__main__":
    main()
