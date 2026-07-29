import React, { useEffect, useState } from 'react';
import { 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer 
} from 'recharts';
import { Activity, CheckCircle, XCircle } from 'lucide-react';
import './App.css';

function App() {
  const [events, setEvents] = useState([]);

  useEffect(() => {
    const sse = new EventSource("http://localhost:8000/stream");
    
    sse.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        console.log("Received event:", data);
        setEvents(prev => [data, ...prev].slice(0, 50)); // Keep last 50 events
      } catch (err) {
        console.error("Error parsing SSE data", err);
      }
    };

    sse.onerror = (err) => {
      console.error("SSE Error:", err);
    };

    return () => {
      sse.close();
    };
  }, []);

  const chartData = [...events].reverse().map((e, idx) => ({
    time: idx,
    price: e.trade_candidate?.price || 0,
    quantity: e.trade_candidate?.quantity || 0,
  }));

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900 p-8">
      <header className="mb-8">
        <h1 className="text-3xl font-bold flex items-center gap-2">
          <Activity className="text-blue-500" /> 
          GridPulse Live Market
        </h1>
        <p className="text-gray-500 mt-2">Continuous Double Auction & Safety Auditor Feed</p>
      </header>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Chart Section */}
        <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-gray-200 p-6">
          <h2 className="text-xl font-semibold mb-4">Trade Candidate Price vs Time</h2>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                <XAxis dataKey="time" hide />
                <YAxis domain={['auto', 'auto']} />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="price" stroke="#3b82f6" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Feed Section */}
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 overflow-hidden flex flex-col h-[500px]">
          <h2 className="text-xl font-semibold mb-4 border-b pb-4">Live Safety Approvals</h2>
          <div className="overflow-y-auto flex-1 pr-2 space-y-4">
            {events.length === 0 ? (
              <p className="text-gray-400 text-center mt-10">Waiting for trades...</p>
            ) : (
              events.map((e, i) => (
                <div key={i} className="p-4 rounded-lg bg-gray-50 border border-gray-100 flex items-start gap-3">
                  {e.status === 'approved' ? (
                    <CheckCircle className="text-green-500 shrink-0 mt-1" size={20} />
                  ) : (
                    <XCircle className="text-red-500 shrink-0 mt-1" size={20} />
                  )}
                  <div>
                    <div className="flex justify-between items-center mb-1">
                      <span className={`font-semibold text-sm ${e.status === 'approved' ? 'text-green-700' : 'text-red-700'}`}>
                        {e.status.toUpperCase()}
                      </span>
                      <span className="text-xs font-mono text-gray-400" title={e.trace_id}>
                        {e.trace_id?.substring(0,8)}...
                      </span>
                    </div>
                    <p className="text-sm">
                      <span className="font-medium">Qty:</span> {e.trade_candidate?.quantity?.toFixed(2)} | 
                      <span className="font-medium ml-2">Price:</span> ${e.trade_candidate?.price?.toFixed(2)}
                    </p>
                    {e.reason && (
                      <p className="text-xs text-red-600 mt-1">Reason: {e.reason}</p>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
