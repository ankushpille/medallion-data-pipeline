import { useState, useEffect } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Grid, Input, Badge } from '../components/UI';

export default function Clients() {
  const [clients, setClients] = useState([]);
  const [dsClient, setDsClient] = useState('');
  const [datasets, setDatasets] = useState([]);
  const [dsLoading, setDsLoading] = useState(false);
  const toast = useToast();
  const nav = useNavigate();
  const loc = useLocation();

  const loadClients = async () => {
    try {
      const r = await apiCall('/config/clients');
      setClients(r.clients || []);
    } catch { toast('Failed to load clients', 'error'); }
  };

  const loadDs = async (c) => {
    if (!c) return;
    setDsLoading(true);
    try {
      const r = await apiCall(`/config/datasets?client_name=${c}`);
      setDatasets(r.datasets || []);
    } catch { toast('Failed to load datasets', 'error'); }
    finally { setDsLoading(false); }
  };

  useEffect(() => {
    loadClients();
    if (loc.state?.client) {
      setDsClient(loc.state.client);
      loadDs(loc.state.client);
    }
  }, []);

  const handleClientClick = (c) => { setDsClient(c); loadDs(c); };

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>👥 Clients & Datasets</div>
        <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Browse all registered clients and their ingested datasets.</div>
      </div>

      <Grid cols={2} gap={18}>
        {/* Clients */}
        <Card>
          <CardHeader icon="🏢" iconColor="blue" title="All clients">
            <Btn size="sm" onClick={loadClients}>↻ Refresh</Btn>
          </CardHeader>
          <CardBody>
            {clients.length === 0
              ? <div style={{ color: 'var(--text2)', fontSize: 13 }}>No clients yet — run an ingestion first.</div>
              : (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 12 }}>
                  {clients.map(c => (
                    <div key={c} onClick={() => handleClientClick(c)} style={{
                      display: 'inline-flex', alignItems: 'center', gap: 5,
                      padding: '6px 14px', background: 'var(--bg)',
                      border: `1.5px solid ${dsClient === c ? 'var(--accent)' : 'var(--border2)'}`,
                      borderRadius: 20, fontSize: 13, fontWeight: 600, cursor: 'pointer',
                      color: dsClient === c ? 'var(--blue)' : 'var(--text)',
                      background: dsClient === c ? 'var(--blue-bg)' : 'var(--bg)',
                      transition: 'all .15s',
                    }}>
                      <div style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--green)' }} />
                      {c}
                    </div>
                  ))}
                </div>
              )
            }
            <div style={{ padding: '10px 14px', borderRadius: 'var(--radius-sm)', fontSize: 12.5, background: 'var(--blue-bg)', color: 'var(--blue)', borderLeft: '3px solid var(--accent)' }}>
              💡 Click a client name to view their datasets →
            </div>
          </CardBody>
        </Card>

        {/* Datasets */}
        <Card>
          <CardHeader icon="📦" iconColor="amber" title="Datasets" />
          <CardBody>
            <div style={{ display: 'flex', gap: 8, marginBottom: 16, alignItems: 'center' }}>
              <Input value={dsClient} onChange={e => setDsClient(e.target.value)} placeholder="Enter client name e.g. AMGEN" style={{ flex: 1 }} />
              <Btn onClick={() => loadDs(dsClient)}>Load</Btn>
            </div>

            {dsLoading && <div style={{ color: 'var(--text2)', fontSize: 13 }}>Loading...</div>}

            {!dsLoading && datasets.length === 0 && dsClient && (
              <div style={{ color: 'var(--text2)', fontSize: 13 }}>No datasets found for {dsClient}.</div>
            )}

            {!dsLoading && datasets.length === 0 && !dsClient && (
              <div style={{ color: 'var(--text2)', fontSize: 13 }}>Pick a client to see their datasets.</div>
            )}

            {datasets.map(d => (
              <div key={d.dataset_id} style={{
                padding: '12px 0', borderBottom: '1px solid var(--border)',
                display: 'flex', alignItems: 'flex-start', gap: 12,
              }}>
                <div style={{
                  width: 34, height: 34, background: 'var(--amber-bg)',
                  borderRadius: 8, display: 'flex', alignItems: 'center',
                  justifyContent: 'center', fontSize: 15, flexShrink: 0,
                }}>📄</div>
                <div style={{ flex: 1 }}>
                  <div style={{ fontWeight: 700, fontSize: 13.5, marginBottom: 2 }}>{d.dataset_name}</div>
                  <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: 'var(--text3)', marginBottom: 6 }}>{d.dataset_id}</div>
                  <div style={{ display: 'flex', gap: 6 }}>
                    <Btn size="sm" onClick={() => nav('/dq', { state: { id: d.dataset_id } })}>Configure DQ →</Btn>
                    <Btn size="sm" onClick={() => nav('/pipeline', { state: { ds: d } })}>Run Pipeline →</Btn>
                  </div>
                </div>
              </div>
            ))}
          </CardBody>
        </Card>
      </Grid>
    </div>
  );
}
