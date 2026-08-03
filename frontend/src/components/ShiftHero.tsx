import React from 'react';
import { Play, Square, RefreshCw, Clock, Server, Users } from 'lucide-react';
import { AppState } from '../types';

interface ShiftHeroProps {
  state: AppState;
  onStart: () => void;
  onStop: () => void;
  onToggleShift: () => void;
}

// Dynamic color palette for N workers
const WORKER_COLORS = [
  { bg: 'rgba(0, 136, 204, 0.2)', color: '#38bdf8', border: 'rgba(0, 136, 204, 0.4)', gradient: 'linear-gradient(90deg, #0088cc, #00f2fe)' },
  { bg: 'rgba(121, 40, 202, 0.2)', color: '#c084fc', border: 'rgba(121, 40, 202, 0.4)', gradient: 'linear-gradient(90deg, #7928ca, #c084fc)' },
  { bg: 'rgba(16, 185, 129, 0.2)', color: '#34d399', border: 'rgba(16, 185, 129, 0.4)', gradient: 'linear-gradient(90deg, #10b981, #34d399)' },
  { bg: 'rgba(245, 158, 11, 0.2)', color: '#fbbf24', border: 'rgba(245, 158, 11, 0.4)', gradient: 'linear-gradient(90deg, #f59e0b, #fbbf24)' },
  { bg: 'rgba(239, 68, 68, 0.2)', color: '#f87171', border: 'rgba(239, 68, 68, 0.4)', gradient: 'linear-gradient(90deg, #ef4444, #f87171)' },
];

function getWorkerColor(index: number) {
  return WORKER_COLORS[index % WORKER_COLORS.length];
}

function getWorkerDisplayName(workerId: string): string {
  return workerId.replace('worker-', 'Worker ').toUpperCase();
}

export default function ShiftHero({ state, onStart, onStop, onToggleShift }: ShiftHeroProps) {
  const { is_running, remaining_seconds, total_shift_seconds } = state;
  const activeConsumer = state.active_consumer || `worker-${state.active_server}`;
  const aliveWorkers = state.alive_workers || [];
  const totalWorkers = aliveWorkers.length || 1;

  const formatTime = (secs: number) => {
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    return `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  };

  const progressPercent = total_shift_seconds > 0 
    ? Math.min(100, Math.max(0, ((total_shift_seconds - remaining_seconds) / total_shift_seconds) * 100))
    : 0;

  const activeIdx = aliveWorkers.indexOf(activeConsumer);
  const activeColor = getWorkerColor(activeIdx >= 0 ? activeIdx : 0);

  return (
    <div className="shift-hero-grid">
      <div className="card">
        <div className="card-title">
          <Clock size={20} className="text-primary" />
          Active Rotation Shift Monitor
        </div>

        <div className="server-status-banner">
          <div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '4px' }}>ACTIVE CONSUMER</div>
            <div className="server-pill" style={{ 
              background: activeColor.bg, 
              color: activeColor.color, 
              border: `1px solid ${activeColor.border}` 
            }}>
              <Server size={14} />
              {getWorkerDisplayName(activeConsumer)} (ACTIVE)
            </div>
          </div>

          <div style={{ textAlign: 'right' }}>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '4px' }}>
              {totalWorkers > 1 ? 'RESTING WORKERS' : 'NO OTHER WORKERS'}
            </div>
            <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              {aliveWorkers.filter(w => w !== activeConsumer).map((wid, i) => {
                const color = getWorkerColor(aliveWorkers.indexOf(wid));
                return (
                  <div key={wid} className="server-pill" style={{ 
                    opacity: 0.6, 
                    background: color.bg, 
                    color: color.color, 
                    border: `1px solid ${color.border}`,
                    fontSize: '0.75rem',
                    padding: '4px 10px'
                  }}>
                    {getWorkerDisplayName(wid)} (REST)
                  </div>
                );
              })}
              {aliveWorkers.filter(w => w !== activeConsumer).length === 0 && (
                <div className="server-pill" style={{ opacity: 0.4, fontSize: '0.75rem', padding: '4px 10px' }}>
                  SINGLE WORKER MODE
                </div>
              )}
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: '20px' }}>
          <div>
            <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Shift Countdown Timer (10 Min Cycle)</div>
            <div className="timer-display">{formatTime(remaining_seconds || 0)}</div>
          </div>

          <div className="btn-group">
            {!is_running ? (
              <button className="btn btn-primary" onClick={onStart}>
                <Play size={18} /> Start Rotation
              </button>
            ) : (
              <button className="btn btn-danger" onClick={onStop}>
                <Square size={18} /> Pause Engine
              </button>
            )}
            <button className="btn btn-secondary" onClick={onToggleShift} title="Force rotate to next worker">
              <RefreshCw size={18} /> Rotate Worker
            </button>
          </div>
        </div>

        {/* Shift Progress Bar */}
        <div style={{ width: '100%', height: '6px', background: 'rgba(255,255,255,0.06)', borderRadius: '4px', marginTop: '20px', overflow: 'hidden' }}>
          <div 
            style={{ 
              width: `${progressPercent}%`, 
              height: '100%', 
              background: activeColor.gradient,
              transition: 'width 1s linear'
            }} 
          />
        </div>
      </div>

      <div className="card" style={{ display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
        <div>
          <div className="card-title">
            <Server size={20} /> Architecture Overview
          </div>
          <p style={{ fontSize: '0.88rem', color: 'var(--text-muted)', lineHeight: '1.6' }}>
            <strong>N-Worker Dynamic Rotation:</strong><br />
            {totalWorkers} worker{totalWorkers > 1 ? 's' : ''} registered. 
            {totalWorkers > 1 
              ? ` Consumer role rotates every 10 minutes. Listeners run on all workers continuously.`
              : ` Single worker handles both listening and replying.`
            }
          </p>

          <div style={{ marginTop: '12px' }}>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '6px' }}>
              <Users size={14} style={{ verticalAlign: 'middle', marginRight: '4px' }} />
              Registered Workers ({totalWorkers})
            </div>
            <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
              {aliveWorkers.map((wid, i) => {
                const color = getWorkerColor(i);
                const isActive = wid === activeConsumer;
                return (
                  <div key={wid} className="server-pill" style={{ 
                    background: color.bg, 
                    color: color.color, 
                    border: `1px solid ${color.border}`,
                    fontSize: '0.72rem',
                    padding: '3px 10px',
                    opacity: isActive ? 1 : 0.6,
                  }}>
                    <span style={{ 
                      width: '6px', height: '6px', 
                      borderRadius: '50%', 
                      background: isActive ? '#22c55e' : '#6b7280',
                      display: 'inline-block',
                      boxShadow: isActive ? '0 0 6px #22c55e' : 'none'
                    }}/>
                    {getWorkerDisplayName(wid)}
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        <div style={{ background: 'rgba(0,0,0,0.2)', padding: '12px 16px', borderRadius: '10px', fontSize: '0.82rem', color: 'var(--text-muted)', marginTop: '16px' }}>
          <div>⚡ Human Typing Simulation: Active</div>
          <div>🛡️ Rate-Limit Protection: Auto FloodWait Wait</div>
          <div>🎯 Min Accounts Per Group: 1 Listener + 1 Replier</div>
        </div>
      </div>
    </div>
  );
}
