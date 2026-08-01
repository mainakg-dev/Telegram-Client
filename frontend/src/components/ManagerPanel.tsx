import React, { useState } from 'react';
import { MessageSquare, Target as TargetIcon, Plus, Trash2 } from 'lucide-react';
import { Message, Target } from '../types';

interface ManagerPanelProps {
  messages: Message[];
  targets: Target[];
  onAddMessage: (content: string) => void;
  onDeleteMessage: (id: number) => void;
  onAddTarget: (username: string) => void;
  onDeleteTarget: (id: number) => void;
}

export default function ManagerPanel({
  messages,
  targets,
  onAddMessage,
  onDeleteMessage,
  onAddTarget,
  onDeleteTarget
}: ManagerPanelProps) {
  const [newMsgContent, setNewMsgContent] = useState('');
  const [newTargetUsername, setNewTargetUsername] = useState('');

  const handleAddMsg = (e: React.FormEvent) => {
    e.preventDefault();
    if (!newMsgContent.trim()) return;
    onAddMessage(newMsgContent.trim());
    setNewMsgContent('');
  };

  const handleAddTarget = (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTargetUsername.trim()) return;
    onAddTarget(newTargetUsername.trim());
    setNewTargetUsername('');
  };

  return (
    <div className="grid-2col">
      {/* Predefined Messages Manager */}
      <div className="card">
        <div className="card-title">
          <MessageSquare size={18} />
          Pre-defined Message Templates ({messages.length})
        </div>

        <form onSubmit={handleAddMsg} className="input-group">
          <input 
            type="text" 
            className="input" 
            placeholder="Add predefined message..." 
            value={newMsgContent}
            onChange={(e) => setNewMsgContent(e.target.value)}
          />
          <button type="submit" className="btn btn-primary" style={{ padding: '8px 16px' }}>
            <Plus size={16} /> Add
          </button>
        </form>

        <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
          {messages.map((m) => (
            <div key={m.id} className="list-item">
              <span style={{ color: 'white', flex: 1, paddingRight: '10px' }}>{m.content}</span>
              <button 
                onClick={() => onDeleteMessage(m.id)} 
                style={{ background: 'none', border: 'none', color: '#f87171', cursor: 'pointer' }}
              >
                <Trash2 size={16} />
              </button>
            </div>
          ))}
        </div>
      </div>

      {/* Target Telegram Channels / Users Manager */}
      <div className="card">
        <div className="card-title">
          <TargetIcon size={18} />
          Target Telegram Groups / Channels ({targets.length})
        </div>

        <form onSubmit={handleAddTarget} className="input-group">
          <input 
            type="text" 
            className="input" 
            placeholder="Username (e.g. @LinX013 or @telegram)..." 
            value={newTargetUsername}
            onChange={(e) => setNewTargetUsername(e.target.value)}
          />
          <button type="submit" className="btn btn-primary" style={{ padding: '8px 16px' }}>
            <Plus size={16} /> Add Target
          </button>
        </form>

        <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
          {targets.map((t) => (
            <div key={t.id} className="list-item">
              <span style={{ color: 'var(--primary)', fontWeight: '600', flex: 1 }}>{t.username}</span>
              <button 
                onClick={() => onDeleteTarget(t.id)} 
                style={{ background: 'none', border: 'none', color: '#f87171', cursor: 'pointer' }}
              >
                <Trash2 size={16} />
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
