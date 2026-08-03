import React, { useEffect, useRef } from 'react';
import { Terminal } from 'lucide-react';
import { Log } from '../types';

interface LogsTerminalProps {
  logs: Log[];
}

export default function LogsTerminal({ logs }: LogsTerminalProps) {
  const terminalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (terminalRef.current) {
      terminalRef.current.scrollTop = 0;
    }
  }, [logs]);

  const getWorkerColor = (group?: number) => {
    const colors = ['#38bdf8', '#c084fc', '#34d399', '#fbbf24', '#f87171'];
    if (!group || group < 1) return '#94a3b8';
    return colors[(group - 1) % colors.length];
  };

  return (
    <div className="card">
      <div className="card-title" style={{ justifyContent: 'space-between' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Terminal size={18} />
          Live Rotation Logs Terminal
        </div>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>Auto-streaming WebSocket</span>
      </div>

      <div className="terminal-box" ref={terminalRef}>
        {logs && logs.length > 0 ? (
          logs.map((log) => (
            <div key={log.id} className="terminal-line">
              <span className="log-time">[{new Date(log.timestamp).toLocaleTimeString()}]</span>
              <span style={{ color: getWorkerColor(log.server_group), fontWeight: '600' }}>
                [W{log.server_group || '?'}]
              </span>
              <span className={`log-${log.status}`}>[{log.action}]</span>
              <span style={{ color: 'white' }}>{log.account_phone ? `${log.account_phone}:` : ''}</span>
              <span style={{ color: 'var(--text-muted)' }}>{log.details}</span>
              {log.target && <span style={{ color: 'var(--primary)' }}>({log.target})</span>}
            </div>
          ))
        ) : (
          <div style={{ color: 'var(--text-muted)', textAlign: 'center', marginTop: '40px' }}>
            No logs captured yet. Start rotation to stream activity.
          </div>
        )}
      </div>
    </div>
  );
}
