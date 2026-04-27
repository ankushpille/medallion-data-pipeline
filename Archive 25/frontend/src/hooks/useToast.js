import { createContext, useContext, useState, useCallback } from 'react';

const ToastCtx = createContext(null);

export function ToastProvider({ children }) {
  const [toasts, setToasts] = useState([]);

  const toast = useCallback((msg, type = 'info') => {
    const id = Date.now();
    setToasts(t => [...t, { id, msg, type }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), 3500);
  }, []);

  return (
    <ToastCtx.Provider value={toast}>
      {children}
      <div style={{ position: 'fixed', bottom: 24, right: 24, display: 'flex', flexDirection: 'column', gap: 8, zIndex: 9999 }}>
        {toasts.map(t => (
          <div key={t.id} style={{
            padding: '12px 18px',
            borderRadius: 12,
            fontSize: 13.5,
            fontWeight: 600,
            fontFamily: "'Bricolage Grotesque', sans-serif",
            maxWidth: 340,
            boxShadow: '0 4px 16px rgba(0,0,0,.12)',
            animation: 'fadeIn .25s ease both',
            background: t.type === 'success' ? 'var(--green-bg)' : t.type === 'error' ? 'var(--red-bg)' : 'var(--blue-bg)',
            color: t.type === 'success' ? 'var(--green)' : t.type === 'error' ? 'var(--red)' : 'var(--blue)',
            border: `1.5px solid ${t.type === 'success' ? 'var(--green-bdr)' : t.type === 'error' ? 'var(--red-bdr)' : 'var(--blue-bdr)'}`,
          }}>{t.msg}</div>
        ))}
      </div>
    </ToastCtx.Provider>
  );
}

export function useToast() {
  return useContext(ToastCtx);
}
