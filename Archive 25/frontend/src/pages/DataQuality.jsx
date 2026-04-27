import { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Input, FormGroup, Badge, Table } from '../components/UI';

export default function DataQuality() {
  const [id, setId] = useState('');
  const [cols, setCols] = useState([]);
  const [loaded, setLoaded] = useState(false);
  const [aiRunning, setAiRunning] = useState(false);
  const [pipeRunning, setPipeRunning] = useState(false);
  const [msg, setMsg] = useState('');
  const toast = useToast();
  const loc = useLocation();

  useEffect(() => {
    if (loc.state?.id) { setId(loc.state.id); }
  }, [loc.state]);

  const loadConfig = async () => {
    if (!id.trim()) { toast('Paste a dataset ID first', 'error'); return; }
    setMsg('Loading...');
    try {
      const r = await apiCall(`/dq/config/${id.trim()}`);
      setCols(r.columns || []);
      setLoaded(true);
      setMsg('');
    } catch (e) { setMsg('Failed to load: ' + e.message); }
  };

  const aiSuggest = async () => {
    if (!id.trim()) { toast('Paste a dataset ID first', 'error'); return; }
    setAiRunning(true);
    setMsg('✨ AI is analysing your dataset columns...');
    try {
      const r = await apiCall('/dq/suggest', 'POST', { dataset_id: id.trim(), mode: 'auto' });
      setMsg('');
      if (r.detail) { toast('Error: ' + r.detail, 'error'); return; }
      toast(`AI done — ${r.inserted || 0} rules added`, 'success');
      loadConfig();
    } catch (e) { toast('Error: ' + e.message, 'error'); setMsg(''); }
    finally { setAiRunning(false); }
  };

  const runPipeline = async () => {
    if (!id.trim()) { toast('Paste a dataset ID first', 'error'); return; }
    setPipeRunning(true);
    toast('Running pipeline...', 'info');
    try {
      const r = await apiCall(`/pipeline/run/${id.trim()}`, 'POST');
      if (r.detail) { toast('Error: ' + r.detail, 'error'); return; }
      const m = r.metrics || {};
      toast(`Done — ${m.silver?.rows_written || 0} silver rows written`, 'success');
    } catch (e) { toast('Error: ' + e.message, 'error'); }
    finally { setPipeRunning(false); }
  };

  const sevBadge = (s) => {
    if (s === 'ERROR') return <Badge variant="red">ERROR</Badge>;
    if (s === 'WARNING') return <Badge variant="amber">WARNING</Badge>;
    return <Badge variant="gray">{s || '—'}</Badge>;
  };

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>✅ Data Quality</div>
        <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Configure quality rules for your datasets. AI can suggest them automatically.</div>
      </div>

      <Card>
        <CardHeader icon="🎯" iconColor="purple" title="Load dataset" />
        <CardBody>
          <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', flexWrap: 'wrap' }}>
            <div style={{ flex: 1, minWidth: 280 }}>
              <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>Dataset ID</label>
              <div style={{ fontSize: 11.5, color: 'var(--text2)', marginBottom: 5 }}>Get this from 👥 Clients → select a dataset</div>
              <Input value={id} onChange={e => setId(e.target.value)} placeholder="Paste the dataset_id here" />
            </div>
            <Btn onClick={loadConfig}>Load Rules</Btn>
            <Btn variant="primary" onClick={aiSuggest} disabled={aiRunning}>
              {aiRunning ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Thinking...</> : '✨ AI Suggest'}
            </Btn>
            <Btn variant="green" onClick={runPipeline} disabled={pipeRunning}>
              {pipeRunning ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Running...</> : '▶ Run Pipeline'}
            </Btn>
          </div>
          {msg && <div style={{ marginTop: 10, fontSize: 13, color: 'var(--text2)' }}>{msg}</div>}
        </CardBody>
      </Card>

      {loaded && (
        <Card>
          <CardHeader icon="📋" iconColor="green" title="DQ rules by column">
            <Badge variant="blue">{cols.length} columns</Badge>
          </CardHeader>
          <CardBody>
            <Table
              headers={['Column name', 'Data type', 'Rules applied', 'Severity', 'Status']}
              rows={cols.map(c => [
                <span style={{ fontWeight: 600 }}>{c.column_name}</span>,
                <Badge variant="blue">{c.expected_data_type || '—'}</Badge>,
                <span style={{ fontSize: 12.5 }}>{(c.dq_rules || []).join(', ') || 'No rules yet'}</span>,
                sevBadge(c.severity),
                <Badge variant={c.is_active ? 'green' : 'gray'}>{c.is_active ? '✓ Active' : 'Inactive'}</Badge>,
              ])}
              emptyMsg="No columns found for this dataset."
            />
          </CardBody>
        </Card>
      )}
    </div>
  );
}
