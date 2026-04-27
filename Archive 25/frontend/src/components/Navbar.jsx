import { useState, useEffect } from 'react';
import { NavLink } from 'react-router-dom';
import { getBase, setBase, apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';

const tabs = [
  { to: '/',         label: '🏠 Home' },
  { to: '/ingest',   label: '⬇ Ingest' },
  { to: '/pipeline', label: '▶ Pipeline' },
  { to: '/clients',  label: '👥 Clients' },
  { to: '/apis',     label: '🔌 API Sources' },
  { to: '/dq',       label: '✅ Data Quality' },
  { to: '/browse',   label: '🔍 Browse' },
  { to: '/history',  label: '📜 History' },
];

export default function Navbar() {
  const [url, setUrl] = useState(getBase());
  const [online, setOnline] = useState(null);
  const toast = useToast();

  const check = async (base) => {
    try {
      await apiCall('/health');
      setOnline(true);
      toast('Server is healthy ✓', 'success');
    } catch {
      setOnline(false);
      toast('Cannot reach server', 'error');
    }
  };

  useEffect(() => { check(getBase()); }, []);

  const handleSet = () => {
    setBase(url);
    check(url);
  };

  return (
    <nav style={{
      background: '#fff',
      borderBottom: '1px solid var(--border)',
      padding: '0 24px',
      display: 'flex',
      alignItems: 'center',
      height: 58,
      position: 'sticky',
      top: 0,
      zIndex: 100,
      boxShadow: 'var(--shadow)',
      gap: 0,
    }}>
      {/* Logo */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginRight: 32, flexShrink: 0 }}>
        <div style={{
          width: 34, height: 34, background: 'var(--accent)',
          borderRadius: 9, display: 'flex', alignItems: 'center',
          justifyContent: 'center', color: '#fff', fontWeight: 800, fontSize: 16,
        }}>D</div>
        <div>
          <div style={{ fontWeight: 700, fontSize: 14.5 }}>Data Engineer Agent</div>
          <div style={{ fontSize: 10.5, color: 'var(--text3)' }}>Azure · FastAPI · LangGraph</div>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: 'flex', gap: 2, flex: 1, overflow: 'auto' }}>
        {tabs.map(t => (
          <NavLink key={t.to} to={t.to} end={t.to === '/'} style={({ isActive }) => ({
            padding: '7px 13px',
            borderRadius: 'var(--radius-sm)',
            fontSize: 13,
            fontWeight: 500,
            color: isActive ? 'var(--blue)' : 'var(--text2)',
            background: isActive ? 'var(--blue-bg)' : 'transparent',
            textDecoration: 'none',
            display: 'flex',
            alignItems: 'center',
            gap: 5,
            whiteSpace: 'nowrap',
            transition: 'all .15s',
          })}>{t.label}</NavLink>
        ))}
      </div>

      {/* Right */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
        {/* Server badge */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 5,
          padding: '5px 11px', borderRadius: 20,
          fontSize: 12, fontWeight: 600,
          border: `1px solid ${online ? 'var(--green-bdr)' : 'var(--border)'}`,
          background: online ? 'var(--green-bg)' : 'var(--bg)',
          color: online ? 'var(--green)' : 'var(--text2)',
        }}>
          <div style={{
            width: 7, height: 7, borderRadius: '50%',
            background: online ? 'var(--green)' : 'var(--text3)',
            animation: online ? 'blink 2s infinite' : 'none',
          }} />
          {online === null ? 'Checking...' : online ? 'Online' : 'Offline'}
        </div>

        {/* URL bar */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 6,
          background: 'var(--bg)', border: '1.5px solid var(--border)',
          borderRadius: 'var(--radius-sm)', padding: '5px 5px 5px 12px',
        }}>
          <input
            value={url}
            onChange={e => setUrl(e.target.value)}
            style={{
              background: 'transparent', border: 'none', outline: 'none',
              fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace",
              color: 'var(--text)', width: 190,
            }}
            onKeyDown={e => e.key === 'Enter' && handleSet()}
          />
          <button onClick={handleSet} style={{
            padding: '4px 10px', borderRadius: 6, border: '1.5px solid var(--border2)',
            background: 'transparent', cursor: 'pointer', fontSize: 12, fontWeight: 600,
            fontFamily: "'Bricolage Grotesque', sans-serif",
          }}>Set</button>
        </div>
      </div>
    </nav>
  );
}
