import React from 'react';
import { Server, MessageSquare, Trash2, Radio, Reply } from 'lucide-react';
import { Account } from '../types';

interface AccountGridProps {
  accounts: Account[];
  activeServer: number;
  activeConsumer?: string;
  onDeleteAccount: (id: number) => void;
}

// Same color palette as ShiftHero for consistency
const WORKER_COLORS = [
  { bg: 'rgba(0, 136, 204, 0.2)', color: '#38bdf8', border: 'rgba(0, 136, 204, 0.4)' },
  { bg: 'rgba(121, 40, 202, 0.2)', color: '#c084fc', border: 'rgba(121, 40, 202, 0.4)' },
  { bg: 'rgba(16, 185, 129, 0.2)', color: '#34d399', border: 'rgba(16, 185, 129, 0.4)' },
  { bg: 'rgba(245, 158, 11, 0.2)', color: '#fbbf24', border: 'rgba(245, 158, 11, 0.4)' },
  { bg: 'rgba(239, 68, 68, 0.2)', color: '#f87171', border: 'rgba(239, 68, 68, 0.4)' },
];

export default function AccountGrid({ accounts, activeServer, activeConsumer, onDeleteAccount }: AccountGridProps) {
  // Group accounts by role instead of server_group
  const listeners = accounts.filter(a => a.role === 'LISTENER');
  const repliers = accounts.filter(a => a.role === 'REPLIER');
  // Accounts with no role yet fall into repliers
  const unassigned = accounts.filter(a => !a.role);

  const renderAccountCard = (acc: Account) => {
    const isTyping = acc.status === 'TYPING';
    const isListener = acc.role === 'LISTENER';

    return (
      <div 
        key={acc.id} 
        className={`account-card ${acc.status === 'ACTIVE' ? 'active-shift' : ''} ${isTyping ? 'typing' : ''}`}
        style={{ position: 'relative' }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span className="account-name">Acc #{acc.id}</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            {isTyping && <MessageSquare size={12} style={{ color: 'var(--accent-amber)' }} />}
            <button
              onClick={(e) => {
                e.stopPropagation();
                if (window.confirm(`Delete account #${acc.id} (${acc.phone})?`)) {
                  onDeleteAccount(acc.id);
                }
              }}
              title="Delete account"
              style={{
                background: 'none',
                border: 'none',
                color: '#f87171',
                cursor: 'pointer',
                padding: '2px',
                display: 'flex',
                alignItems: 'center',
                opacity: 0.7,
                transition: 'opacity 0.2s'
              }}
              onMouseEnter={(e) => (e.currentTarget.style.opacity = '1')}
              onMouseLeave={(e) => (e.currentTarget.style.opacity = '0.7')}
            >
              <Trash2 size={13} />
            </button>
          </div>
        </div>
        <div className="account-phone">{acc.phone}</div>
        <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap' }}>
          <div className={`status-badge ${acc.status}`}>{acc.status}</div>
          {acc.role && (
            <div className="status-badge" style={{
              background: isListener ? 'rgba(0, 136, 204, 0.2)' : 'rgba(121, 40, 202, 0.15)',
              color: isListener ? '#38bdf8' : '#c084fc',
            }}>
              {isListener ? '👂 LISTENER' : '💬 REPLIER'}
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <div className="servers-container">
      {/* Listener Accounts Panel */}
      <div className="card">
        <div className="server-column-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: '600' }}>
            <Radio size={18} style={{ color: '#38bdf8' }} />
            Listener Accounts ({listeners.length})
          </div>
          <span className="server-pill" style={{ 
            fontSize: '0.7rem', 
            background: 'rgba(0, 136, 204, 0.15)', 
            color: '#38bdf8', 
            border: '1px solid rgba(0, 136, 204, 0.3)' 
          }}>
            ALWAYS ACTIVE
          </span>
        </div>

        <div className="account-grid">
          {listeners.map(renderAccountCard)}
          {listeners.length === 0 && (
            <div style={{ color: 'var(--text-muted)', fontSize: '0.85rem', padding: '20px', textAlign: 'center', gridColumn: '1 / -1' }}>
              No listener accounts assigned yet. Start the rotator to auto-assign.
            </div>
          )}
        </div>
      </div>

      {/* Replier Accounts Panel */}
      <div className="card">
        <div className="server-column-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: '600' }}>
            <Reply size={18} style={{ color: '#c084fc' }} />
            Replier Accounts ({repliers.length + unassigned.length})
          </div>
          <span className="server-pill" style={{ 
            fontSize: '0.7rem', 
            background: 'rgba(121, 40, 202, 0.15)', 
            color: '#c084fc', 
            border: '1px solid rgba(121, 40, 202, 0.3)' 
          }}>
            ROUND-ROBIN ASSIGNED
          </span>
        </div>

        <div className="account-grid">
          {repliers.map(renderAccountCard)}
          {unassigned.map(renderAccountCard)}
          {repliers.length === 0 && unassigned.length === 0 && (
            <div style={{ color: 'var(--text-muted)', fontSize: '0.85rem', padding: '20px', textAlign: 'center', gridColumn: '1 / -1' }}>
              No replier accounts available.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
