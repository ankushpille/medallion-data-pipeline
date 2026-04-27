// ── Shared UI components ──────────────────────────────────────────────────────

export function Card({ children, style }) {
  return (
    <div style={{
      background: 'var(--surface)',
      border: '1px solid var(--border)',
      borderRadius: 'var(--radius)',
      boxShadow: 'var(--shadow)',
      overflow: 'hidden',
      marginBottom: 18,
      ...style,
    }}>{children}</div>
  );
}

export function CardHeader({ icon, iconColor = 'blue', title, children }) {
  const colors = {
    blue: 'var(--blue-bg)', green: 'var(--green-bg)',
    amber: 'var(--amber-bg)', purple: 'var(--purple-bg)', red: 'var(--red-bg)',
  };
  return (
    <div style={{
      padding: '14px 20px',
      borderBottom: '1px solid var(--border)',
      display: 'flex', alignItems: 'center', gap: 10,
    }}>
      <div style={{
        width: 30, height: 30, borderRadius: 8,
        background: colors[iconColor] || colors.blue,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 14, flexShrink: 0,
      }}>{icon}</div>
      <div style={{ fontSize: 14, fontWeight: 700, flex: 1 }}>{title}</div>
      {children}
    </div>
  );
}

export function CardBody({ children, style }) {
  return <div style={{ padding: 20, ...style }}>{children}</div>;
}

export function Btn({ children, variant = 'outline', size = 'md', onClick, disabled, style, full }) {
  const base = {
    display: 'inline-flex', alignItems: 'center', gap: 6,
    border: 'none', cursor: disabled ? 'not-allowed' : 'pointer',
    fontFamily: "'Bricolage Grotesque', sans-serif",
    fontWeight: 600, transition: 'all .15s', opacity: disabled ? .6 : 1,
    width: full ? '100%' : undefined, justifyContent: full ? 'center' : undefined,
    borderRadius: 'var(--radius-sm)',
    padding: size === 'sm' ? '5px 12px' : '8px 16px',
    fontSize: size === 'sm' ? 12 : 13,
  };
  const variants = {
    primary: { background: 'var(--accent)', color: '#fff' },
    green: { background: 'var(--green)', color: '#fff' },
    outline: { background: 'transparent', color: 'var(--text)', border: '1.5px solid var(--border2)' },
    red: { background: 'var(--red-bg)', color: 'var(--red)', border: '1.5px solid var(--red-bdr)' },
    ghost: { background: 'var(--surface2)', color: 'var(--text2)', border: '1px solid var(--border)' },
  };
  return (
    <button onClick={onClick} disabled={disabled} style={{ ...base, ...variants[variant], ...style }}>
      {children}
    </button>
  );
}

export function Badge({ children, variant = 'gray' }) {
  const v = {
    green:  { bg: 'var(--green-bg)',  color: 'var(--green)',  bdr: 'var(--green-bdr)' },
    blue:   { bg: 'var(--blue-bg)',   color: 'var(--blue)',   bdr: 'var(--blue-bdr)' },
    amber:  { bg: 'var(--amber-bg)',  color: 'var(--amber)',  bdr: 'var(--amber-bdr)' },
    red:    { bg: 'var(--red-bg)',    color: 'var(--red)',    bdr: 'var(--red-bdr)' },
    purple: { bg: 'var(--purple-bg)', color: 'var(--purple)', bdr: 'var(--purple-bdr)' },
    gray:   { bg: 'var(--surface2)', color: 'var(--text2)',  bdr: 'var(--border)' },
  }[variant] || { bg: 'var(--surface2)', color: 'var(--text2)', bdr: 'var(--border)' };
  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 3,
      padding: '3px 9px', borderRadius: 20,
      fontSize: 11.5, fontWeight: 600,
      background: v.bg, color: v.color, border: `1px solid ${v.bdr}`,
    }}>{children}</span>
  );
}

export function FormGroup({ label, hint, children }) {
  return (
    <div style={{ marginBottom: 14 }}>
      {label && <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>{label}</label>}
      {hint && <div style={{ fontSize: 11.5, color: 'var(--text2)', marginBottom: 5 }}>{hint}</div>}
      {children}
    </div>
  );
}

const inputStyle = {
  width: '100%', background: 'var(--bg)',
  border: '1.5px solid var(--border)', borderRadius: 'var(--radius-sm)',
  padding: '8px 12px', color: 'var(--text)', fontSize: 13,
  fontFamily: "'Bricolage Grotesque', sans-serif", outline: 'none',
  transition: 'all .15s',
};

export function Input({ value, onChange, placeholder, type = 'text', onFocus, onBlur, style }) {
  return (
    <input
      type={type} value={value} onChange={onChange} placeholder={placeholder}
      style={{ ...inputStyle, ...style }}
      onFocus={e => { e.target.style.borderColor = 'var(--accent)'; e.target.style.background = '#fff'; e.target.style.boxShadow = '0 0 0 3px rgba(45,91,227,.1)'; onFocus?.(); }}
      onBlur={e => { e.target.style.borderColor = 'var(--border)'; e.target.style.background = 'var(--bg)'; e.target.style.boxShadow = 'none'; onBlur?.(); }}
    />
  );
}

export function Select({ value, onChange, children, style }) {
  return (
    <select value={value} onChange={onChange} style={{ ...inputStyle, cursor: 'pointer', ...style }}
      onFocus={e => { e.target.style.borderColor = 'var(--accent)'; }}
      onBlur={e => { e.target.style.borderColor = 'var(--border)'; }}
    >{children}</select>
  );
}

export function InfoBox({ children, variant = 'tip' }) {
  const v = {
    tip:  { bg: 'var(--blue-bg)',  color: 'var(--blue)',  bdr: 'var(--accent)' },
    warn: { bg: 'var(--amber-bg)', color: 'var(--amber)', bdr: 'var(--amber)' },
    ok:   { bg: 'var(--green-bg)', color: 'var(--green)', bdr: 'var(--green)' },
  }[variant];
  return (
    <div style={{
      padding: '10px 14px', borderRadius: 'var(--radius-sm)',
      fontSize: 13, lineHeight: 1.5, marginBottom: 14,
      borderLeft: `3px solid ${v.bdr}`,
      background: v.bg, color: v.color,
    }}>{children}</div>
  );
}

export function Grid({ cols = 2, gap = 18, children, style }) {
  return (
    <div style={{ display: 'grid', gridTemplateColumns: `repeat(${cols}, 1fr)`, gap, ...style }}>
      {children}
    </div>
  );
}

export function Mono({ children }) {
  return <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: 'var(--text2)' }}>{children}</span>;
}

export function MetricBar({ label, value, max, color = 'var(--green)' }) {
  const pct = max ? Math.round((value / max) * 100) : 0;
  return (
    <div style={{ marginBottom: 12 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12.5, marginBottom: 4 }}>
        <span style={{ fontWeight: 600 }}>{label}</span>
        <span>{pct}%</span>
      </div>
      <div style={{ height: 8, background: 'var(--surface2)', borderRadius: 4, overflow: 'hidden', border: '1px solid var(--border)' }}>
        <div style={{ height: '100%', width: pct + '%', background: color, borderRadius: 4, transition: 'width .8s ease' }} />
      </div>
    </div>
  );
}

export function Table({ headers, rows, emptyMsg = 'No data.' }) {
  return (
    <div style={{ overflow: 'auto', borderRadius: 'var(--radius-sm)', border: '1px solid var(--border)' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            {headers.map((h, i) => (
              <th key={i} style={{
                padding: '10px 14px', textAlign: 'left',
                fontSize: 11.5, fontWeight: 700, color: 'var(--text2)',
                background: 'var(--bg)', borderBottom: '1px solid var(--border)',
                textTransform: 'uppercase', letterSpacing: '.04em',
              }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr><td colSpan={headers.length} style={{ padding: '14px', color: 'var(--text2)', fontSize: 13 }}>{emptyMsg}</td></tr>
          ) : rows.map((row, i) => (
            <tr key={i} style={{ cursor: 'default' }}
              onMouseEnter={e => Array.from(e.currentTarget.cells).forEach(c => c.style.background = 'var(--bg)')}
              onMouseLeave={e => Array.from(e.currentTarget.cells).forEach(c => c.style.background = '')}
            >
              {row.map((cell, j) => (
                <td key={j} style={{
                  padding: '12px 14px', fontSize: 13, color: 'var(--text)',
                  borderBottom: i < rows.length - 1 ? '1px solid var(--border)' : 'none',
                  verticalAlign: 'middle',
                }}>{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function LayerRow({ emoji, title, desc, status = 'Active', statusColor = 'green' }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 12,
      padding: '11px 14px', background: 'var(--bg)',
      borderRadius: 'var(--radius-sm)', border: '1px solid var(--border)',
      marginBottom: 8,
    }}>
      <span style={{ fontSize: 18 }}>{emoji}</span>
      <div style={{ flex: 1 }}>
        <div style={{ fontWeight: 700, fontSize: 13.5 }}>{title}</div>
        <div style={{ fontSize: 12, color: 'var(--text2)' }}>{desc}</div>
      </div>
      <Badge variant={statusColor}>{status}</Badge>
    </div>
  );
}

export function PipelineTrack({ stages }) {
  const colors = { done: 'var(--green)', active: 'var(--accent)', pending: 'var(--text3)' };
  return (
    <div style={{
      display: 'flex', alignItems: 'center',
      background: 'var(--bg)', border: '1px solid var(--border)',
      borderRadius: 40, padding: '5px 8px', marginBottom: 16,
    }}>
      {stages.map((s, i) => (
        <div key={i} style={{
          flex: 1, textAlign: 'center', padding: '6px 8px',
          borderRadius: 30, fontSize: 12, fontWeight: 600,
          color: s.status === 'active' ? '#fff' : colors[s.status],
          background: s.status === 'active' ? 'var(--accent)' : s.status === 'done' ? 'var(--green-bg)' : 'transparent',
          display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 5,
          transition: 'all .3s',
        }}>
          <div style={{
            width: 7, height: 7, borderRadius: '50%',
            background: 'currentColor', opacity: .6,
            animation: s.status === 'active' ? 'blink .8s infinite' : 'none',
          }} />
          {s.label}
        </div>
      ))}
    </div>
  );
}
