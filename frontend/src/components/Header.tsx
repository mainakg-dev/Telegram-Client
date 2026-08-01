import React from 'react';
import { Send, ShieldCheck, UserPlus } from 'lucide-react';

interface HeaderProps {
  isRunning: boolean;
  activeServer: number;
  onOpenLoginModal: () => void;
}

export default function Header({ isRunning, onOpenLoginModal }: HeaderProps) {
  return (
    <header className="header">
      <div className="brand">
        <div className="brand-icon">
          <Send size={24} />
        </div>
        <div>
          <h1 className="brand-title">Telegram Client Rotation Engine</h1>
          <p className="brand-subtitle">Automated Multi-Account Shift Manager & Human Typing Simulator</p>
        </div>
      </div>
      <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
        <button 
          onClick={onOpenLoginModal}
          className="btn btn-primary"
          style={{ fontSize: '0.85rem', padding: '8px 14px' }}
        >
          <UserPlus size={16} />
          Connect Telegram Account
        </button>
        <div className="server-pill" style={{ background: isRunning ? 'rgba(16, 185, 129, 0.15)' : 'rgba(239, 68, 68, 0.15)', color: isRunning ? '#34d399' : '#f87171', border: '1px solid currentColor' }}>
          <ShieldCheck size={14} />
          {isRunning ? 'ROTATION ENGINE ONLINE' : 'ENGINE PAUSED'}
        </div>
      </div>
    </header>
  );
}
