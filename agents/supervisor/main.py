import time
import sys
import os
import uuid
import json
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared.utils.redis_client import RedisManager
from shared.checkpoint.manager import CheckpointManager

HEARTBEAT_TIMEOUT = 15 # seconds
RATE_LIMIT_MINS = 10
MAX_HEALS = 3
HIGH_RISK_AGENTS = ["market", "safety", "settlement"]
APPROVAL_TIMEOUT = 300 # 5 minutes

class Supervisor:
    def __init__(self):
        self.redis_mgr = RedisManager()
        self.checkpoint_mgr = CheckpointManager()
        self.agent_last_heartbeat = {}
        self.agent_heal_history = {} # agent -> list of timestamps
        
        # Mapping of agents to their streams/groups for checkpointing
        self.agent_streams = {
            "demand": {"ingestion_stream": "demand_group"},
            "market": {"demand_stream": "market_group", "supply_stream": "market_group"}
        }
        
    def check_health(self):
        now = time.time()
        for agent, last_hb in list(self.agent_last_heartbeat.items()):
            if now - last_hb > HEARTBEAT_TIMEOUT:
                print(f"[{datetime.utcnow()}] Supervisor: {agent} missed heartbeat! (timeout={HEARTBEAT_TIMEOUT}s)")
                self.initiate_heal(agent, "Missing heartbeat")

    def initiate_heal(self, agent, reason):
        now = datetime.utcnow()
        # Rate limit check
        history = self.agent_heal_history.get(agent, [])
        history = [t for t in history if now - t < timedelta(minutes=RATE_LIMIT_MINS)]
        
        if len(history) >= MAX_HEALS:
            print(f"[{now}] Supervisor: {agent} exceeded max heal attempts ({MAX_HEALS} per {RATE_LIMIT_MINS}m). Escalating to human!")
            return
            
        history.append(now)
        self.agent_heal_history[agent] = history
        del self.agent_last_heartbeat[agent] # clear until it comes back

        session_id = str(uuid.uuid4())
        print(f"[{now}] Supervisor: Initiating healing session {session_id} for {agent}...")
        self.checkpoint_mgr.log_event(session_id, agent, "remediation_started", {"reason": reason})

        # 1. Checkpoint
        streams = self.agent_streams.get(agent, {})
        checkpoint_id = self.checkpoint_mgr.snapshot(agent, session_id, reason, streams)
        self.checkpoint_mgr.log_event(session_id, agent, "checkpoint_created", {"checkpoint_id": checkpoint_id})
        print(f"[{now}] Supervisor: Checkpoint created: {checkpoint_id}")

        # If high risk, inject healing state BEFORE restarting so it picks it up on boot
        if agent in HIGH_RISK_AGENTS:
            self.redis_mgr.r.set(f"healing_state:{agent}", session_id)

        # 2. Soft Kill (Publish to control_commands)
        self.redis_mgr.publish("control_commands", {"agent": agent, "command": "restart"})
        print(f"[{now}] Supervisor: Sent restart command to {agent}")

        # 3. Validate (Wait for heartbeat to resume)
        self.checkpoint_mgr.log_event(session_id, agent, "validating")
        print(f"[{now}] Supervisor: Waiting for {agent} to resume heartbeats...")
        
        timeout = 30 # wait 30s for restart
        start_wait = time.time()
        success = False
        while time.time() - start_wait < timeout:
            msgs = self.redis_mgr.r.xread({"heartbeats": "$"}, count=10, block=1000)
            if msgs:
                for stream, msg_list in msgs:
                    for msg_id, data in msg_list:
                        hb_agent = data.get(b'agent', data.get('agent', b''))
                        if isinstance(hb_agent, bytes):
                            hb_agent = hb_agent.decode()
                        if hb_agent == agent:
                            success = True
                            self.agent_last_heartbeat[agent] = time.time()
                            break
            if success:
                break
                
        if success:
            print(f"[{datetime.utcnow()}] Supervisor: {agent} successfully validated (heartbeat resumed)!")
            self.checkpoint_mgr.log_event(session_id, agent, "validation_passed")
        else:
            print(f"[{datetime.utcnow()}] Supervisor: {agent} failed to resume! Moving to human approval anyway...")
            self.checkpoint_mgr.log_event(session_id, agent, "validation_failed")

        ckpt = self.checkpoint_mgr.session.query(AgentCheckpoint).filter_by(checkpoint_id=checkpoint_id).first()
        
        ckpt.status = "awaiting_approval"
        self.checkpoint_mgr.session.commit()
        self.checkpoint_mgr.log_event(session_id, agent, "awaiting_approval")
        print(f"[{datetime.utcnow()}] Supervisor: Awaiting human approval for {APPROVAL_TIMEOUT}s before finalizing...")
        
        # Wait for approval
        approved = False
        decision_made = False
        approval_start = time.time()
        last_app_id = "$"
        try:
            # dummy push to create stream if not exists
            mid = self.redis_mgr.r.xadd("human_approvals", {"init": "1"})
            self.redis_mgr.r.xdel("human_approvals", mid)
        except: pass
        
        while time.time() - approval_start < APPROVAL_TIMEOUT:
            msgs = self.redis_mgr.r.xread({"human_approvals": last_app_id}, count=10, block=1000)
            if msgs:
                for s, msg_list in msgs:
                    for msg_id, data in msg_list:
                        last_app_id = msg_id
                        
                        s_id = data.get(b'session_id', data.get('session_id', b''))
                        if isinstance(s_id, bytes): s_id = s_id.decode()
                        
                        dec = data.get(b'decision', data.get('decision', b''))
                        if isinstance(dec, bytes): dec = dec.decode()
                        
                        if s_id == session_id:
                            if dec == "approve":
                                approved = True
                            elif dec == "reject":
                                approved = False
                            decision_made = True
                            break
                    if decision_made: break
            if decision_made: break
        
        if approved:
            print(f"[{datetime.utcnow()}] Supervisor: Healing session {session_id} APPROVED.")
            self.checkpoint_mgr.log_event(session_id, agent, "approved")
            self.checkpoint_mgr.log_event(session_id, agent, "committed")
            ckpt.status = "committed"
            self.checkpoint_mgr.session.commit()
            self.redis_mgr.r.delete(f"healing_state:{agent}")
        else:
            print(f"[{datetime.utcnow()}] Supervisor: Healing session {session_id} REJECTED or TIMED OUT. Rolling back...")
            self.checkpoint_mgr.log_event(session_id, agent, "rejected")
            self.checkpoint_mgr.restore(checkpoint_id, streams)
            self.checkpoint_mgr.log_event(session_id, agent, "rolled_back")
            ckpt.status = "rolled_back"
            self.checkpoint_mgr.session.commit()
            self.redis_mgr.r.delete(f"healing_state:{agent}")
            # Broadcast rollback event for Settlement
            self.redis_mgr.publish("rollbacks", {"session_id": session_id, "agent": agent})


    def run(self):
        print("Supervisor Agent started. Listening for heartbeats...")
        # Create heartbeats stream if it doesn't exist by pushing a dummy and deleting it
        try:
            msg_id = self.redis_mgr.r.xadd("heartbeats", {"init": "1"})
            self.redis_mgr.r.xdel("heartbeats", msg_id)
        except:
            pass

        last_id = "$"
        while True:
            # Check health
            self.check_health()
            
            # Listen for heartbeats
            try:
                messages = self.redis_mgr.r.xread({"heartbeats": last_id}, count=100, block=2000)
                if messages:
                    for stream, msg_list in messages:
                        for msg_id, data in msg_list:
                            last_id = msg_id
                            # data could be bytes or strings depending on redis config
                            agent = data.get(b'agent', data.get('agent', b''))
                            if isinstance(agent, bytes): agent = agent.decode()
                            
                            self.agent_last_heartbeat[agent] = time.time()
            except Exception as e:
                print(f"Supervisor error: {e}")
                time.sleep(1)

if __name__ == "__main__":
    from shared.checkpoint.manager import AgentCheckpoint
    Supervisor().run()
