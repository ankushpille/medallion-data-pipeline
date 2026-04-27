import { useState, useEffect } from 'react';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Grid, Select, Badge, InfoBox, MetricBar } from '../components/UI';

export default function Pipeline() {
  const [clients, setClients] = useState([]);
  const [selClient, setSelClient] = useState('');
  const [datasets, setDatasets] = useState([]);
  const [selDs, setSelDs] = useState('');
  const [metrics, setMetrics] = useState(null);
  const [running, setRunning] = useState(false);
  const toast = useToast();

  useEffect(() => {
    apiCall('/config/clients').then(r => setClients(r.clients || [])).catch(() => {});
  }, []);

  const loadDatasets = async (c) => {
    setSelClient(c); setSelDs(''); setDatasets([]);
    if (!c) return;
    try {
      const r = await apiCall(`/config/datasets?client_name=${c}`);
      setDatasets(r.datasets || []);
    } catch {}
  };

  const sync = async () => {
    if (!selClient) { toast('Select a client first', 'error'); return; }
    toast('Syncing master config...', 'info');
    const r = await apiCall('/dq/sync_master_config', 'POST', { client_name: selClient });
    if (r.detail) { toast('Sync error: ' + r.detail, 'error'); return; }
    toast('Synced for ' + selClient + ' ✓', 'success');
  };

  const runPipeline = async () => {
    if (!selDs) { toast('Select a dataset', 'error'); return; }
    setRunning(true); setMetrics(null);
    toast('Running pipeline...', 'info');
    try {
      const r = await apiCall(`/pipeline/run/${selDs}`, 'POST');
      if (r.detail) { toast('Error: ' + r.detail, 'error'); return; }
      setMetrics(r.metrics || {});
      toast('Pipeline complete! ✓', 'success');
    } catch (e) { toast('Error: ' + e.message, 'error'); }
    finally { setRunning(false); }
  };

  const raw = metrics?.raw?.rows_read || 0;
  const bronze = metrics?.bronze?.rows_written || 0;
  const silver = metrics?.silver?.rows_written || 0;

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>▶ Run Pipeline</div>
        <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Select a client and dataset to run Raw → Bronze → Silver manually.</div>
      </div>

      <Card>
        <CardHeader icon="⚙️" iconColor="amber" title="Select dataset" />
        <CardBody>
          <InfoBox variant="tip">
            💡 After running orchestration, always <strong>Sync</strong> first before running the pipeline.
          </InfoBox>
          <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', flexWrap: 'wrap' }}>
            <div style={{ flex: 1, minWidth: 160 }}>
              <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>Client</label>
              <Select value={selClient} onChange={e => loadDatasets(e.target.value)}>
                <option value="">Choose a client...</option>
                {clients.map(c => <option key={c} value={c}>{c}</option>)}
              </Select>
            </div>
            <div style={{ flex: 1, minWidth: 200 }}>
              <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>Dataset</label>
              <Select value={selDs} onChange={e => setSelDs(e.target.value)}>
                <option value="">Choose a dataset...</option>
                {datasets.map(d => <option key={d.dataset_id} value={d.dataset_id}>{d.dataset_name}</option>)}
              </Select>
            </div>
            <Btn onClick={sync}>↻ Sync First</Btn>
            <Btn variant="primary" onClick={runPipeline} disabled={running}>
              {running ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Running...</> : '▶ Run Pipeline'}
            </Btn>
          </div>
        </CardBody>
      </Card>

      {metrics && (
        <Card>
          <CardHeader icon="📊" iconColor="green" title="Pipeline results">
            <Badge variant="green">SUCCESS</Badge>
          </CardHeader>
          <CardBody>
            <Grid cols={3} gap={14} style={{ marginBottom: 20 }}>
              {[
                { label: 'Raw rows read', val: raw, color: 'var(--text)', bg: 'var(--bg)' },
                { label: 'Bronze written', val: bronze, color: 'var(--amber)', bg: 'var(--amber-bg)' },
                { label: 'Silver written', val: silver, color: 'var(--green)', bg: 'var(--green-bg)' },
              ].map((m, i) => (
                <div key={i} style={{ textAlign: 'center', padding: 16, background: m.bg, borderRadius: 'var(--radius-sm)', border: '1px solid var(--border)' }}>
                  <div style={{ fontSize: 11, fontWeight: 700, textTransform: 'uppercase', color: m.color, opacity: .7, marginBottom: 5 }}>{m.label}</div>
                  <div style={{ fontSize: 28, fontWeight: 700, color: m.color }}>{m.val}</div>
                </div>
              ))}
            </Grid>
            <MetricBar label="Raw → Bronze" value={bronze} max={Math.max(raw, 1)} color="var(--amber)" />
            <MetricBar label="Bronze → Silver (after DQ)" value={silver} max={Math.max(raw, 1)} color="var(--green)" />
          </CardBody>
        </Card>
      )}
    </div>
  );
}
