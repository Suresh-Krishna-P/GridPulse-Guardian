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
                        current_hash=curr_hash
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
