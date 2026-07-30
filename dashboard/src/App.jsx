import React, { useEffect, useState } from 'react';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Area, ComposedChart
} from 'recharts';
import { Activity, CheckCircle, XCircle, Zap, Cpu, Server, Network } from 'lucide-react';
import './App.css';

function App() {
  const [trades, setTrades] = useState([]);
  const [pricing, setPricing] = useState([]);
  const [explanations, setExplanations] = useState({});
  const [lastSync, setLastSync] = useState(null);
  const [healingSessions, setHealingSessions] = useState([]);
  
  // Feed scrolling state
  const isScrolledRef = React.useRef(false);
  const pendingTradesRef = React.useRef([]);
  const [pendingCount, setPendingCount] = useState(0);
  
  // Aggregate stats
  const [totalCleared, setTotalCleared] = useState(0);

  useEffect(() => {
    const sse = new EventSource("http://localhost:8000/stream");
    
    sse.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const type = data._type;
        
        if (type === "safety_results") {
          if (isScrolledRef.current) {
            pendingTradesRef.current.unshift(data);
            setPendingCount(pendingTradesRef.current.length);
          } else {
            setTrades(prev => [data, ...prev].slice(0, 50));
          }
          if (data.status === 'approved' && data.trade_candidate?.quantity) {
             setTotalCleared(prev => prev + data.trade_candidate.quantity);
          }
        } else if (type === "pricing_signals") {
          setPricing(prev => [...prev, data].slice(-50)); // keep last 50 for chart
        } else if (type === "explanations") {
          setExplanations(prev => ({
            ...prev,
            [data.trace_id]: data.explanation
          }));
        } else if (type === "federated_model_updates") {
          setLastSync(new Date().toLocaleTimeString());
        }
      } catch (err) {
        console.error("Error parsing SSE data", err);
      }
    };

    const fetchSessions = async () => {
      try {
        const res = await fetch("http://localhost:8000/healing_sessions");
        const data = await res.json();
        setHealingSessions(data);
      } catch (err) {
        console.error("Failed to fetch healing sessions", err);
      }
    };
    fetchSessions();
    const interval = setInterval(fetchSessions, 2000);

    return () => {
      sse.close();
      clearInterval(interval);
    };
  }, []);

  const handleOverride = async (traceId, decision) => {
    try {
      await fetch("http://localhost:8000/override", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trace_id: traceId, decision })
      });
      // Optionally remove from UI immediately or wait for SSE update
      setTrades(prev => prev.filter(t => t.trace_id !== traceId || t.status !== 'pending_review'));
    } catch (err) {
      console.error("Override failed", err);
    }
  };

  const handleHealApproval = async (sessionId, decision) => {
    try {
        await fetch("http://localhost:8000/heal_approval", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ session_id: sessionId, decision })
        });
    } catch(err) {
        console.error("Heal approval failed", err);
    }
  };

  const handleFeedScroll = (e) => {
    if (e.target.scrollTop > 5) {
      isScrolledRef.current = true;
    } else {
      isScrolledRef.current = false;
      if (pendingTradesRef.current.length > 0) {
        setTrades(prev => [...pendingTradesRef.current, ...prev].slice(0, 50));
        pendingTradesRef.current = [];
        setPendingCount(0);
      }
    }
  };

  // Prepare chart data mapping over trades since load tests only emit trades
  const chartData = [...trades].reverse().map((t, i) => {
    // Try to find a matching pricing bound
    const p = pricing[pricing.length - 1 - i] || pricing[pricing.length - 1] || { recommended_floor: 10, recommended_ceiling: 40 };
    return {
      time: i,
      floor: p.recommended_floor,
      ceiling: p.recommended_ceiling,
      clearingPrice: t?.trade_candidate?.price || null,
    };
  });

  const latestPricing = pricing[pricing.length - 1] || { recommended_floor: 0, recommended_ceiling: 0 };

  return (
    <div className="min-h-screen p-8">
      <header className="mb-8 flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold flex items-center gap-2 text-white">
            <Activity className="text-blue-500" /> 
            GridPulse Command Center
          </h1>
          <p className="text-gray-400 mt-2">Autonomous Market & Safety Auditor Feed</p>
        </div>
        
        {/* Top KPIs */}
        <div className="flex gap-6">
          <div className="glass-panel text-center px-6 py-2">
            <div className="text-sm text-gray-400 font-semibold mb-1">Total Cleared</div>
            <div className="text-xl font-bold text-green-400">{totalCleared.toFixed(0)} kW</div>
          </div>
          <div className="glass-panel text-center px-6 py-2">
            <div className="text-sm text-gray-400 font-semibold mb-1">RL Price Window</div>
            <div className="text-xl font-bold text-blue-400">
              ${latestPricing.recommended_floor.toFixed(1)} - ${latestPricing.recommended_ceiling.toFixed(1)}
            </div>
          </div>
          <div className="glass-panel flex flex-col justify-center px-6 py-2">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-sm text-gray-400 font-semibold">Federated Sync</span>
              {/* Ping Animation */}
              <span className="relative flex h-3 w-3">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-purple-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-3 w-3 bg-purple-500"></span>
              </span>
            </div>
            <div className="text-sm font-mono text-purple-400 text-center">{lastSync || "Waiting..."}</div>
          </div>
        </div>
      </header>
      
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        
        {/* Chart Section */}
        <div className="glass-panel lg:col-span-3">
          <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
            <Network className="text-blue-400" size={20}/>
            RL Price Bounds vs Clearing Price
          </h2>
          <div className="h-[400px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={chartData}>
                <defs>
                  <linearGradient id="colorBounds" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.1}/>
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0.0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.1)" vertical={false} />
                <XAxis dataKey="time" hide />
                <YAxis domain={['auto', 'auto']} stroke="#9ca3af" tick={{fill: '#9ca3af'}} />
                <Tooltip 
                  contentStyle={{ backgroundColor: 'rgba(17, 24, 39, 0.9)', borderColor: 'rgba(255,255,255,0.1)', color: '#fff' }}
                  itemStyle={{ color: '#fff' }}
                />
                <Legend />
                <Area type="monotone" dataKey="ceiling" stroke="#60a5fa" fill="url(#colorBounds)" strokeWidth={1} />
                <Area type="monotone" dataKey="floor" stroke="#60a5fa" fill="transparent" strokeWidth={1} />
                <Line type="monotone" dataKey="clearingPrice" name="Clearing Price" stroke="#10b981" strokeWidth={2} dot={false} />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Trade Feed Section */}
        <div className="glass-panel overflow-hidden flex flex-col h-[500px]">
          <h2 className="text-xl font-semibold mb-4 border-b pb-4 flex items-center gap-2">
            <Zap className="text-yellow-400" size={20}/>
            Live Trade Approvals
          </h2>
          <div className="overflow-y-auto flex-1 pr-2 flex flex-col gap-3 relative" onScroll={handleFeedScroll}>
            {pendingCount > 0 && (
               <div className="sticky top-2 z-10 flex justify-center pointer-events-none">
                 <div className="bg-blue-600 text-white text-xs px-4 py-1.5 rounded-full shadow-lg font-bold">
                   {pendingCount} new trades (scroll to top)
                 </div>
               </div>
            )}
            
            {trades.length === 0 ? (
              <p className="text-gray-400 text-center mt-10">Waiting for trades...</p>
            ) : (
              trades.map((e, i) => {
                const isApproved = e.status === 'approved';
                const carbon = e.trade_candidate?.carbon_intensity_gco2 || 0;
                const traceId = e.trace_id;
                const explanation = explanations[traceId];

                return (
                  <div key={i} className="p-4 rounded-lg bg-gray-800/50 border">
                    <div className="flex items-start gap-3">
                      {isApproved ? (
                        <CheckCircle className="text-green-500 shrink-0 mt-1" size={18} />
                      ) : (
                        <XCircle className="text-red-500 shrink-0 mt-1" size={18} />
                      )}
                      <div className="flex-1">
                        <div className="flex justify-between items-center mb-1">
                          <span className={`badge ${isApproved ? 'badge-green' : e.status === 'pending_review' ? 'badge-blue' : 'badge-red'}`}>
                            {e.status === 'pending_review' ? 'PENDING REVIEW' : e.status}
                          </span>
                          <span className="text-xs font-mono text-gray-400" title={traceId}>
                            {traceId?.substring(0,6)}
                          </span>
                        </div>
                        
                        <p className="text-sm mt-2 text-gray-300">
                          <span className="font-medium">Qty:</span> {e.trade_candidate?.quantity?.toFixed(1)} kW &nbsp;|&nbsp; 
                          <span className="font-medium ml-2">Price:</span> ${e.trade_candidate?.price?.toFixed(2)}
                        </p>
                        
                        {/* Carbon Indicator */}
                        {carbon > 0 && (
                          <p className="text-xs mt-1 text-orange-400 font-mono">
                            ⚡ Carbon: {carbon.toFixed(0)} gCO2/kWh
                          </p>
                        )}

                        {/* HITL Override Buttons */}
                        {e.status === 'pending_review' && (
                          <div className="mt-3 flex gap-2">
                            <button 
                              onClick={() => handleOverride(traceId, 'approved')}
                              className="px-3 py-1 bg-green-600 hover:bg-green-500 text-white text-xs font-bold rounded"
                            >
                              Approve
                            </button>
                            <button 
                              onClick={() => handleOverride(traceId, 'rejected')}
                              className="px-3 py-1 bg-red-600 hover:bg-red-500 text-white text-xs font-bold rounded"
                            >
                              Reject
                            </button>
                          </div>
                        )}
                        
                      </div>
                    </div>
                    
                    {/* LLM Explanation Dropdown / Panel */}
                    {explanation && (
                      <div className="mt-3 pt-3 border-t border-gray-700">
                        <div className="flex items-center gap-1 mb-1">
                          <Cpu size={12} className="text-purple-400" />
                          <span className="text-xs text-purple-400 font-semibold uppercase tracking-wider">Ollama AI Analysis</span>
                        </div>
                        <p className="text-xs text-gray-400 italic">"{explanation}"</p>
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </div>
        </div>
      </div>

      {/* Self-Healing Panel */}
      <div className="glass-panel mt-6">
        <h2 className="text-xl font-semibold mb-4 flex items-center gap-2">
          <Server className="text-indigo-400" size={20}/>
          Self-Healing Operations & Rollbacks
        </h2>
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {healingSessions.length === 0 ? (
            <p className="text-gray-400">No recent healing operations.</p>
          ) : (
            healingSessions.map(sess => (
              <div key={sess.session_id} className="p-4 rounded-lg bg-gray-800/50 border border-indigo-900/50">
                <div className="flex justify-between items-center mb-2">
                  <span className="font-bold text-lg">{sess.agent_name.toUpperCase()} Agent</span>
                  <span className={`badge ${sess.status === 'committed' ? 'badge-green' : sess.status === 'rolled_back' ? 'badge-red' : sess.status === 'awaiting_approval' ? 'badge-blue' : 'bg-gray-600'}`}>
                    {sess.status.replace('_', ' ').toUpperCase()}
                  </span>
                </div>
                <p className="text-sm text-gray-400 mb-3">Trigger: {sess.trigger_reason}</p>
                <div className="text-xs font-mono text-gray-500 mb-3">
                  {sess.events.map((e, i) => (
                    <div key={i}>[{e.timestamp.split('T')[1].substring(0,8)}] {e.step}</div>
                  ))}
                </div>
                {sess.status === 'awaiting_approval' && (
                  <div className="mt-3 pt-3 border-t border-gray-700 flex justify-between items-center">
                    <span className="text-xs text-blue-400 animate-pulse">Awaiting Human Approval (Auto-rollback in ~5m)</span>
                    <div className="flex gap-2">
                      <button onClick={() => handleHealApproval(sess.session_id, 'approve')} className="px-3 py-1 bg-green-600 hover:bg-green-500 rounded text-xs font-bold text-white">Approve</button>
                      <button onClick={() => handleHealApproval(sess.session_id, 'reject')} className="px-3 py-1 bg-red-600 hover:bg-red-500 rounded text-xs font-bold text-white">Reject</button>
                    </div>
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
