import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Badge, Grid, LayerRow } from '../components/UI';

function StatCard({ emoji, value, label, color }) {
  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 'var(--radius)', padding: '18px 20px',
      boxShadow: 'var(--shadow)', transition: 'box-shadow .2s',
    }}
      onMouseEnter={e => e.currentTarget.style.boxShadow = 'var(--shadow-md)'}
      onMouseLeave={e => e.currentTarget.style.boxShadow = 'var(--shadow)'}
    >
      <div style={{ fontSize: 22, marginBottom: 8 }}>{emoji}</div>
      <div style={{ fontSize: 30, fontWeight: 700, color, lineHeight: 1, marginBottom: 3 }}>{value}</div>
      <div style={{ fontSize: 12, color: 'var(--text2)', fontWeight: 500 }}>{label}</div>
    </div>
  );
}

function QACard({ emoji, title, desc, onClick }) {
  return (
    <div style={{
      background: 'var(--surface)', border: '1px solid var(--border)',
      borderRadius: 'var(--radius)', padding: 18, cursor: 'pointer',
      boxShadow: 'var(--shadow)', transition: 'all .2s',
    }}
      onClick={onClick}
      onMouseEnter={e => { e.currentTarget.style.boxShadow = 'var(--shadow-md)'; e.currentTarget.style.transform = 'translateY(-2px)'; e.currentTarget.style.borderColor = 'var(--accent)'; }}
      onMouseLeave={e => { e.currentTarget.style.boxShadow = 'var(--shadow)'; e.currentTarget.style.transform = ''; e.currentTarget.style.borderColor = 'var(--border)'; }}
    >
      <div style={{ fontSize: 26, marginBottom: 8 }}>{emoji}</div>
      <div style={{ fontSize: 13.5, fontWeight: 700, marginBottom: 3 }}>{title}</div>
      <div style={{ fontSize: 12, color: 'var(--text2)', lineHeight: 1.5 }}>{desc}</div>
    </div>
  );
}

export default function Dashboard() {
  const [stats, setStats] = useState({ clients: '—', apis: '—', datasets: '—', health: '—' });
  const [clients, setClients] = useState([]);
  const toast = useToast();
  const nav = useNavigate();

  const load = async () => {
    try {
      await apiCall('/health');
      setStats(s => ({ ...s, health: 'Online' }));
    } catch { setStats(s => ({ ...s, health: 'Offline' })); }

    try {
      const r = await apiCall('/config/clients');
      const list = r.clients || [];
      setClients(list);
      setStats(s => ({ ...s, clients: list.length }));
      let tot = 0;
      for (const c of list) {
        try { const ds = await apiCall(`/config/datasets?client_name=${c}`); tot += (ds.datasets || []).length; } catch {}
      }
      setStats(s => ({ ...s, datasets: tot }));
    } catch {}

    try {
      const r = await apiCall('/api-source/list');
      setStats(s => ({ ...s, apis: (r.configs || []).length }));
    } catch {}
  };

  useEffect(() => { load(); }, []);

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-end' }}>
        <div>
          <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>Good day 👋 Pipeline overview</div>
          <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Manage data ingestion from Azure ADLS and REST APIs.</div>
        </div>
        <Btn onClick={() => nav('/history')}>📜 View History</Btn>
      </div>

      {/* Stats */}
      <Grid cols={4} gap={14} style={{ marginBottom: 24 }}>
        <StatCard emoji="🏢" value={stats.clients} label="Clients registered" color="var(--blue)" />
        <StatCard emoji="🔌" value={stats.apis} label="API sources" color="var(--green)" />
        <StatCard emoji="📦" value={stats.datasets} label="Total datasets" color="var(--amber)" />
        <StatCard emoji="💚" value={stats.health} label="Server status" color="var(--green)" />
      </Grid>

      {/* Quick actions */}
      <Grid cols={3} gap={14} style={{ marginBottom: 24 }}>
        <QACard emoji="⬇" title="Run Ingestion" desc="Pull data from Azure Data Lake or any REST API through Raw → Bronze → Silver automatically." onClick={() => nav('/ingest')} />
        <QACard emoji="🔌" title="Connect an API" desc="Register any REST API as a data source. One registration call — no code changes needed." onClick={() => nav('/apis')} />
        <QACard emoji="✅" title="Data Quality Rules" desc="Let AI suggest DQ rules for your dataset columns, then activate them with one click." onClick={() => nav('/dq')} />
        <QACard emoji="📜" title="Execution History" desc="Audit previous pipeline runs, check success rates, and download batch reports." onClick={() => nav('/history')} />
      </Grid>

      <Grid cols={2} gap={18}>
        {/* Clients list */}
        <Card>
          <CardHeader icon="🏢" iconColor="blue" title="Registered clients">
            <Btn size="sm" onClick={load}>↻ Refresh</Btn>
          </CardHeader>
          <CardBody>
            {clients.length === 0
              ? <div style={{ color: 'var(--text2)', fontSize: 13 }}>No clients yet — run an ingestion first.</div>
              : clients.map(c => (
                <div key={c} style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '10px 0', borderBottom: '1px solid var(--border)',
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <div style={{ width: 32, height: 32, background: 'var(--blue-bg)', borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>🏢</div>
                    <div>
                      <div style={{ fontWeight: 700, fontSize: 13.5 }}>{c}</div>
                      <div style={{ fontSize: 11.5, color: 'var(--text2)' }}>Click to view datasets</div>
                    </div>
                  </div>
                  <Btn size="sm" onClick={() => nav('/clients', { state: { client: c } })}>View →</Btn>
                </div>
              ))
            }
          </CardBody>
        </Card>

        {/* Pipeline layers */}
        <Card>
          <CardHeader icon="🔄" iconColor="green" title="Pipeline layers" />
          <CardBody>
            <LayerRow emoji="☁️" title="Landing" desc="Source files arrive — ADLS or API" status="Active" statusColor="green" />
            <LayerRow emoji="📥" title="Raw" desc="Immutable copy, never modified" status="Active" statusColor="green" />
            <LayerRow emoji="🟤" title="Bronze" desc="Standardised Parquet with metadata" status="Active" statusColor="amber" />
            <LayerRow emoji="⚪" title="Silver" desc="DQ-validated, query-ready" status="Active" statusColor="green" />
          </CardBody>
        </Card>
      </Grid>
    </div>
  );
}
