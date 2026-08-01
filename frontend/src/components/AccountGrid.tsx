import React from 'react';
import { Server, MessageSquare, Trash2 } from 'lucide-react';
import { Account } from '../types';

interface AccountGridProps {
  accounts: Account[];
  activeServer: number;
  onDeleteAccount: (id: number) => void;
}

export default function AccountGrid({ accounts, activeServer, onDeleteAccount }: AccountGridProps) {
  const server1Accounts = accounts.filter(a => a.server_group === 1);
  const server2Accounts = accounts.filter(a => a.server_group === 2);

  const renderAccountsColumn = (serverNum: number, accountList: Account[]) => {
    const isActiveServer = activeServer === serverNum;

    return (
      <div className="card">
        <div className="server-column-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontWeight: '600' }}>
            <Server size={18} className={serverNum === 1 ? 'text-primary' : ''} />
            Server {serverNum} Accounts ({accountList.length})
          </div>
          <span className={`server-pill ${serverNum === 1 ? 's1' : 's2'}`} style={{ fontSize: '0.7rem' }}>
            {isActiveServer ? 'WORKING SHIFT' : 'RESTING SHIFT'}
          </span>
        </div>

        <div className="account-grid">
          {accountList.map((acc) => {
            const isTyping = acc.status === 'TYPING';
            return (
              <div 
                key={acc.id} 
                className={`account-card ${isActiveServer ? 'active-shift' : ''} ${isTyping ? 'typing' : ''}`}
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
                <div className={`status-badge ${acc.status}`}>{acc.status}</div>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="servers-container">
      {renderAccountsColumn(1, server1Accounts)}
      {renderAccountsColumn(2, server2Accounts)}
    </div>
  );
}
