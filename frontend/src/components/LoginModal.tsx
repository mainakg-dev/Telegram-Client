import React, { useState } from 'react';
import { X, Smartphone, Key, Lock, CheckCircle, AlertCircle, Loader2 } from 'lucide-react';
import { API_URI } from '../config';

interface LoginModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

export default function LoginModal({ isOpen, onClose, onSuccess }: LoginModalProps) {
  const [step, setStep] = useState<1 | 2 | 3>(1);
  const [phone, setPhone] = useState('');
  const [sessionName, setSessionName] = useState('');
  const [apiId, setApiId] = useState('');
  const [apiHash, setApiHash] = useState('');
  const [serverGroup, setServerGroup] = useState<number>(1);
  const [code, setCode] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [successMsg, setSuccessMsg] = useState('');

  if (!isOpen) return null;

  const handleSendCode = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSuccessMsg('');
    if (!phone.trim()) {
      setError('Phone number is required');
      return;
    }
    setLoading(true);

    try {
      const res = await fetch(`${API_URI}/api/auth/send_code`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          phone: phone.trim(),
          session_name: sessionName.trim() || undefined,
          api_id: apiId.trim() || undefined,
          api_hash: apiHash.trim() || undefined,
          server_group: serverGroup
        })
      });
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.detail || 'Failed to send verification code');
      }

      setSessionName(data.session_name);
      setSuccessMsg(`Verification code sent to ${phone}`);
      setStep(2);
    } catch (err: any) {
      setError(err.message || 'Error connecting to Telegram');
    } finally {
      setLoading(false);
    }
  };

  const handleVerifyCode = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    if (!code.trim()) {
      setError('Verification code is required');
      return;
    }
    setLoading(true);

    try {
      const res = await fetch(`${API_URI}/api/auth/verify_code`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_name: sessionName,
          code: code.trim()
        })
      });
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.detail || 'Failed to verify code');
      }

      if (data.status === 'password_required') {
        setStep(3);
        setSuccessMsg('Two-factor authentication is required for this account.');
      } else if (data.status === 'success') {
        setSuccessMsg(`🎉 Account verified! Connected as ${data.user?.first_name || 'User'}`);
        setTimeout(() => {
          onSuccess();
          resetForm();
          onClose();
        }, 1500);
      }
    } catch (err: any) {
      setError(err.message || 'Verification failed');
    } finally {
      setLoading(false);
    }
  };

  const handleVerifyPassword = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    if (!password) {
      setError('Password is required');
      return;
    }
    setLoading(true);

    try {
      const res = await fetch(`${API_URI}/api/auth/verify_password`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          session_name: sessionName,
          password: password
        })
      });
      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.detail || 'Invalid 2FA password');
      }

      setSuccessMsg(`🎉 Account verified! Connected as ${data.user?.first_name || 'User'}`);
      setTimeout(() => {
        onSuccess();
        resetForm();
        onClose();
      }, 1500);
    } catch (err: any) {
      setError(err.message || '2FA Authentication failed');
    } finally {
      setLoading(false);
    }
  };

  const resetForm = () => {
    setStep(1);
    setPhone('');
    setSessionName('');
    setApiId('');
    setApiHash('');
    setCode('');
    setPassword('');
    setError('');
    setSuccessMsg('');
  };

  return (
    <div className="modal-overlay" style={{
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.75)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 1000,
      backdropFilter: 'blur(4px)'
    }}>
      <div className="card" style={{ width: '100%', maxWidth: '440px', padding: '24px', position: 'relative' }}>
        <button 
          onClick={onClose}
          style={{
            position: 'absolute',
            top: '16px',
            right: '16px',
            background: 'none',
            border: 'none',
            color: '#94a3b8',
            cursor: 'pointer'
          }}
        >
          <X size={20} />
        </button>

        <h3 style={{ margin: '0 0 8px 0', fontSize: '1.25rem', fontWeight: 600, color: 'white', display: 'flex', alignItems: 'center', gap: '8px' }}>
          <Smartphone size={20} style={{ color: 'var(--primary)' }} />
          Connect Telegram Account
        </h3>
        <p style={{ margin: '0 0 20px 0', fontSize: '0.85rem', color: '#94a3b8' }}>
          {step === 1 && 'Enter your Telegram phone number to receive a verification code.'}
          {step === 2 && 'Enter the 5-digit verification code sent to your Telegram app.'}
          {step === 3 && 'Enter your 2-Factor Authentication (Cloud Password).'}
        </p>

        {error && (
          <div style={{
            background: 'rgba(239, 68, 68, 0.15)',
            border: '1px solid rgba(239, 68, 68, 0.4)',
            color: '#f87171',
            padding: '10px 14px',
            borderRadius: '6px',
            fontSize: '0.85rem',
            marginBottom: '16px',
            display: 'flex',
            alignItems: 'center',
            gap: '8px'
          }}>
            <AlertCircle size={16} />
            {error}
          </div>
        )}

        {successMsg && (
          <div style={{
            background: 'rgba(34, 197, 94, 0.15)',
            border: '1px solid rgba(34, 197, 94, 0.4)',
            color: '#4ade80',
            padding: '10px 14px',
            borderRadius: '6px',
            fontSize: '0.85rem',
            marginBottom: '16px',
            display: 'flex',
            alignItems: 'center',
            gap: '8px'
          }}>
            <CheckCircle size={16} />
            {successMsg}
          </div>
        )}

        {step === 1 && (
          <form onSubmit={handleSendCode}>
            <div style={{ marginBottom: '14px' }}>
              <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                Phone Number (with Country Code)
              </label>
              <input 
                type="text" 
                className="input" 
                placeholder="+1234567890" 
                value={phone}
                onChange={(e) => setPhone(e.target.value)}
                required
                style={{ width: '100%' }}
              />
            </div>

            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px', marginBottom: '14px' }}>
              <div>
                <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                  API ID (Optional)
                </label>
                <input 
                  type="text" 
                  className="input" 
                  placeholder="Default: 39865871" 
                  value={apiId}
                  onChange={(e) => setApiId(e.target.value)}
                  style={{ width: '100%' }}
                />
              </div>
              <div>
                <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                  API Hash (Optional)
                </label>
                <input 
                  type="text" 
                  className="input" 
                  placeholder="Default app hash" 
                  value={apiHash}
                  onChange={(e) => setApiHash(e.target.value)}
                  style={{ width: '100%' }}
                />
              </div>
            </div>

            <div style={{ marginBottom: '14px' }}>
              <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                Session Name (Optional)
              </label>
              <input 
                type="text" 
                className="input" 
                placeholder="e.g. acc_s1_01" 
                value={sessionName}
                onChange={(e) => setSessionName(e.target.value)}
                style={{ width: '100%' }}
              />
            </div>

            <div style={{ marginBottom: '20px' }}>
              <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                Worker Group (auto-assigned if left as 1)
              </label>
              <input 
                type="number" 
                className="input" 
                placeholder="1" 
                min={1}
                value={serverGroup}
                onChange={(e) => setServerGroup(Number(e.target.value) || 1)}
                style={{ width: '100%' }}
              />
            </div>

            <button type="submit" className="btn btn-primary" style={{ width: '100%', justifyContent: 'center' }} disabled={loading}>
              {loading ? <Loader2 size={16} className="spin" /> : 'Send Verification Code'}
            </button>
          </form>
        )}

        {step === 2 && (
          <form onSubmit={handleVerifyCode}>
            <div style={{ marginBottom: '20px' }}>
              <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                5-Digit Telegram Verification Code
              </label>
              <div style={{ position: 'relative' }}>
                <input 
                  type="text" 
                  className="input" 
                  placeholder="12345" 
                  value={code}
                  onChange={(e) => setCode(e.target.value)}
                  required
                  style={{ width: '100%', letterSpacing: '4px', textAlign: 'center', fontSize: '1.2rem' }}
                />
                <Key size={18} style={{ position: 'absolute', right: '12px', top: '12px', color: '#64748b' }} />
              </div>
            </div>

            <button type="submit" className="btn btn-primary" style={{ width: '100%', justifyContent: 'center' }} disabled={loading}>
              {loading ? <Loader2 size={16} className="spin" /> : 'Verify Code & Sign In'}
            </button>

            <button 
              type="button" 
              onClick={() => setStep(1)} 
              style={{ background: 'none', border: 'none', color: '#94a3b8', width: '100%', marginTop: '12px', cursor: 'pointer', fontSize: '0.8rem' }}
            >
              ← Back to phone number
            </button>
          </form>
        )}

        {step === 3 && (
          <form onSubmit={handleVerifyPassword}>
            <div style={{ marginBottom: '20px' }}>
              <label style={{ display: 'block', fontSize: '0.8rem', color: '#94a3b8', marginBottom: '6px' }}>
                2FA Cloud Password
              </label>
              <div style={{ position: 'relative' }}>
                <input 
                  type="password" 
                  className="input" 
                  placeholder="Enter 2FA Password" 
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  style={{ width: '100%' }}
                />
                <Lock size={18} style={{ position: 'absolute', right: '12px', top: '12px', color: '#64748b' }} />
              </div>
            </div>

            <button type="submit" className="btn btn-primary" style={{ width: '100%', justifyContent: 'center' }} disabled={loading}>
              {loading ? <Loader2 size={16} className="spin" /> : 'Submit Password'}
            </button>
          </form>
        )}
      </div>
    </div>
  );
}
