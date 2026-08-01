import React from 'react';
import { Play, Square, RefreshCw, Clock, Server } from 'lucide-react';
import { AppState } from '../types';

interface ShiftHeroProps {
  state: AppState;
  onStart: () => void;
  onStop: () => void;
  onToggleShift: () => void;
}

export default function ShiftHero({ state, onStart, onStop, onToggleShift }: ShiftHeroProps) {
  const { is_running, active_server, remaining_seconds, total_shift_seconds } = state;

  const formatTime = (secs: number) => {
    const m = Math.floor(secs / 60);
    const s = secs % 60;
    return `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  };

  const progressPercent = total_shift_seconds > 0 
    ? Math.min(100, Math.max(0, ((total_shift_seconds - remaining_seconds) / total_shift_seconds) * 100))
    : 0;

  return (
    <div className="shift-hero-grid">
      <div className="card">
        <div className="card-title">
          <Clock size={20} className="text-primary" />
          Active Rotation Shift Monitor
        </div>

        <div className="server-status-banner">
          <div>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '4px' }}>CURRENT ACTIVE SERVER</div>
            <div className={`server-pill ${active_server === 1 ? 's1' : 's2'}`}>
              <Server size={14} />
              SERVER {active_server} (SHIFTER)
            </div>
          </div>

          <div style={{ textAlign: 'right' }}>
            <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: '4px' }}>RESTING SERVER</div>
            <div className="server-pill" style={{ opacity: 0.6 }}>
              SERVER {active_server === 1 ? 2 : 1} (RESTING)
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
            <button className="btn btn-secondary" onClick={onToggleShift} title="Force switch between Server 1 and Server 2">
              <RefreshCw size={18} /> Switch Server
            </button>
          </div>
        </div>

        {/* Shift Progress Bar */}
        <div style={{ width: '100%', height: '6px', background: 'rgba(255,255,255,0.06)', borderRadius: '4px', marginTop: '20px', overflow: 'hidden' }}>
          <div 
            style={{ 
              width: `${progressPercent}%`, 
              height: '100%', 
              background: active_server === 1 ? 'linear-gradient(90deg, #0088cc, #00f2fe)' : 'linear-gradient(90deg, #7928ca, #c084fc)',
              transition: 'width 1s linear'
            }} 
          />
        </div>
      </div>

      <div className="card" style={{ display: 'flex', flexDirection: 'column', justifyContent: 'space-between' }}>
        <div>
          <div className="card-title">
            <Server size={20} /> Shift Architecture Overview
          </div>
          <p style={{ fontSize: '0.88rem', color: 'var(--text-muted)', lineHeight: '1.6' }}>
            <strong>Server 1 & Server 2 Dual Rotation:</strong><br />
            Accounts alternate every 10 minutes continuously. While one server group sends messages, the other group rests to prevent spam flags.
          </p>
        </div>

        <div style={{ background: 'rgba(0,0,0,0.2)', padding: '12px 16px', borderRadius: '10px', fontSize: '0.82rem', color: 'var(--text-muted)' }}>
          <div>⚡ Human Typing Simulation: Active</div>
          <div>🛡️ Rate-Limit Protection: Auto FloodWait Wait</div>
        </div>
      </div>
    </div>
  );
}
