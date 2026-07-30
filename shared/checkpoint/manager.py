import os
import uuid
import json
import hashlib
from sqlalchemy import create_engine, Column, String, Text, LargeBinary, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from shared.utils.redis_client import RedisManager

DB_USER = os.getenv("DB_USER", "gridpulse")
DB_PASS = os.getenv("DB_PASS", "gridpulse")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "gridpulse")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
Base = declarative_base()

class AgentCheckpoint(Base):
    __tablename__ = 'agent_checkpoints'
    checkpoint_id = Column(String, primary_key=True)
    agent_name = Column(String, nullable=False)
    healing_session_id = Column(String, nullable=False)
    trigger_reason = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    redis_consumer_offsets = Column(JSONB, nullable=False)
    agent_state_blob = Column(LargeBinary, nullable=True)
    watermark_trace_id = Column(String, nullable=False)
    checksum = Column(String, nullable=False)
    status = Column(String, nullable=False, default='pending')

class HealingEvent(Base):
    __tablename__ = 'healing_events'
    event_id = Column(String, primary_key=True)
    healing_session_id = Column(String, nullable=False)
    agent_name = Column(String, nullable=False)
    step = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    detail = Column(JSONB, nullable=True)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

class CheckpointManager:
    def __init__(self):
        self.redis_mgr = RedisManager()
        self.session = Session()

    def snapshot(self, agent_name, healing_session_id, trigger_reason, streams_to_groups, state_blob=None, watermark_trace_id="unknown"):
        offsets = {}
        for stream, group in streams_to_groups.items():
            # Get group info
            try:
                info = self.redis_mgr.r.xinfo_groups(stream)
                for grp in info:
                    if grp['name'] == group:
                        offsets[stream] = grp['last-delivered-id']
            except Exception as e:
                print(f"Failed to get xinfo for {stream}: {e}")
                
        # Generate checksum
        data_to_hash = json.dumps(offsets, sort_keys=True) + watermark_trace_id
        if state_blob:
            data_to_hash += str(state_blob)
        checksum = hashlib.sha256(data_to_hash.encode()).hexdigest()
        
        checkpoint_id = str(uuid.uuid4())
        
        ckpt = AgentCheckpoint(
            checkpoint_id=checkpoint_id,
            agent_name=agent_name,
            healing_session_id=healing_session_id,
            trigger_reason=trigger_reason,
            redis_consumer_offsets=offsets,
            agent_state_blob=state_blob,
            watermark_trace_id=watermark_trace_id,
            checksum=checksum,
            status='pending'
        )
        self.session.add(ckpt)
        self.session.commit()
        return checkpoint_id
        
    def restore(self, checkpoint_id, streams_to_groups):
        ckpt = self.session.query(AgentCheckpoint).filter_by(checkpoint_id=checkpoint_id).first()
        if not ckpt:
            raise ValueError("Checkpoint not found")
            
        # Verify checksum
        data_to_hash = json.dumps(ckpt.redis_consumer_offsets, sort_keys=True) + ckpt.watermark_trace_id
        if ckpt.agent_state_blob:
            data_to_hash += str(ckpt.agent_state_blob)
        checksum = hashlib.sha256(data_to_hash.encode()).hexdigest()
        
        if checksum != ckpt.checksum:
            raise ValueError("Checksum validation failed. Corrupt checkpoint!")
            
        # Restore offsets
        for stream, offset in ckpt.redis_consumer_offsets.items():
            group = streams_to_groups.get(stream)
            if group:
                try:
                    self.redis_mgr.r.xgroup_setid(stream, group, offset)
                    print(f"Restored offset {offset} for {stream}/{group}")
                except Exception as e:
                    print(f"Restore failed for {stream}/{group}: {e}")
                    
        return ckpt

    def log_event(self, healing_session_id, agent_name, step, detail=None):
        evt = HealingEvent(
            event_id=str(uuid.uuid4()),
            healing_session_id=healing_session_id,
            agent_name=agent_name,
            step=step,
            detail=detail or {}
        )
        self.session.add(evt)
        self.session.commit()
