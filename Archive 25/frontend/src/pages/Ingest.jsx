import { useState, useRef } from 'react';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Grid, FormGroup, Input, PipelineTrack } from '../components/UI';

const INIT_STAGES = [
  { label: 'Discover', status: 'pending' },
  { label: 'Raw', status: 'pending' },
  { label: 'Bronze', status: 'pending' },
  { label: 'Silver', status: 'pending' },
];

export default function Ingest() {
  const [srcType, setSrcType] = useState('ADLS');
  const [client, setClient] = useState('');
  const [folder, setFolder] = useState('');
  const [batch, setBatch] = useState('');
  const [stages, setStages] = useState(INIT_STAGES);
  const [logs, setLogs] = useState([{ time: '--:--:--', msg: 'Ready — fill the form and click Start.', cls: 'info' }]);
  const [results, setResults] = useState(null);
  const [running, setRunning] = useState(false);
  const logRef = useRef(null);
  const toast = useToast();

  const addLog = (msg, cls = 'info') => {
    const time = new Date().toLocaleTimeString();
    setLogs(l => [...l, { time, msg, cls }]);
    setTimeout(() => logRef.current?.scrollTo(0, logRef.current.scrollHeight), 50);
  };

  const setStage = (idx, status) => {
    setStages(s => s.map((st, i) => i === idx ? { ...st, status } : st));
  };

  const delay = ms => new Promise(r => setTimeout(r, ms));

  const run = async () => {
    if (!client.trim() || !folder.trim()) { toast('Enter client name and folder/endpoint', 'error'); return; }
    setRunning(true);
    setStages(INIT_STAGES);
    setResults(null);
    setLogs([]);
    addLog(`Starting: ${srcType} · ${client} · ${folder}`);

    let url = `/orchestrate/run?source_type=${srcType}&client_name=${encodeURIComponent(client)}&folder_path=${encodeURIComponent(folder)}`;
    if (batch.trim()) url += `&batch_id=${encodeURIComponent(batch)}`;

    try {
      setStage(0, 'active');
      addLog('Discovering datasets...');
      const res = await apiCall(url, 'POST');
      setStage(0, 'done'); setStage(1, 'active');
      addLog('Landing to Raw layer...');
      await delay(300);
      setStage(1, 'done'); setStage(2, 'active');
      addLog('Writing Bronze layer...');
      await delay(300);
      setStage(2, 'done'); setStage(3, 'active');
      addLog('Writing Silver layer...');
      await delay(300);
      setStage(3, 'done');

      if (res.detail) { addLog('Error: ' + res.detail, 'error'); toast(res.detail, 'error'); return; }

      const results = res.pipeline_results || [];
      addLog(`Done! ${results.length} dataset(s) processed.`, 'success');
      results.forEach(r => {
        const m = r.metrics || {};
        addLog(`  ${r.dataset_name || r.dataset_id}: raw=${m.raw?.rows_read || 0}  bronze=${m.bronze?.rows_written || 0}  silver=${m.silver?.rows_written || 0}`, 'success');
      });
      setResults(results);
      toast('Ingestion complete! ✓', 'success');
    } catch (e) {
      addLog('Failed: ' + e.message, 'error');
      toast('Network error — check server', 'error');
    } finally {
      setRunning(false);
    }
  };

  const logColors = { info: 'var(--text2)', success: '#4ec99a', error: '#f47474', warn: '#e8a84a' };

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>⬇ Run Ingestion</div>
        <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Choose source, enter client and path, then run the full pipeline end-to-end.</div>
      </div>

      <Grid cols={2} gap={18}>
        {/* Form */}
        <Card>
          <CardHeader icon="⚙️" iconColor="blue" title="Ingestion settings" />
          <CardBody>
            {/* Source selector */}
            <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text2)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: '.04em' }}>Source type</div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 10, marginBottom: 20 }}>
              {[
                { key: 'ADLS', icon: '☁️', name: 'Azure ADLS', desc: 'Files in Azure Data Lake Gen2' },
                { key: 'API',  icon: '🔌', name: 'REST API',  desc: 'Live data from registered API' },
              ].map(s => (
                <div key={s.key}
                  onClick={() => { setSrcType(s.key); setFolder(''); }}
                  style={{
                    border: `2px solid ${srcType === s.key ? 'var(--accent)' : 'var(--border)'}`,
                    borderRadius: 'var(--radius)', padding: 14, cursor: 'pointer',
                    background: srcType === s.key ? 'var(--blue-bg)' : 'var(--bg)',
                    transition: 'all .15s',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 5 }}>
                    <span style={{ fontSize: 20 }}>{s.icon}</span>
                    <span style={{ fontWeight: 700, fontSize: 13.5 }}>{s.name}</span>
                  </div>
                  <div style={{ fontSize: 12, color: 'var(--text2)' }}>{s.desc}</div>
                </div>
              ))}
            </div>

            <FormGroup label="Client name" hint="Who does this data belong to? e.g. AMGEN, CDC">
              <Input value={client} onChange={e => setClient(e.target.value)} placeholder="e.g. AMGEN" />
            </FormGroup>

            <FormGroup
              label="Folder or endpoint path"
              hint={srcType === 'ADLS' ? "Subfolder inside the client's ADLS directory" : 'API endpoint name registered for this client'}
            >
              <Input value={folder} onChange={e => setFolder(e.target.value)} placeholder={srcType === 'ADLS' ? 'e.g. clinical' : 'e.g. users'} />
            </FormGroup>

            <FormGroup label="Batch ID (optional)" hint="Leave empty to auto-generate based on date and time">
              <Input value={batch} onChange={e => setBatch(e.target.value)} placeholder="Auto-generated if empty" />
            </FormGroup>

            <Btn variant="primary" full onClick={run} disabled={running}>
              {running ? <><span className="spinner" style={{ width: 14, height: 14 }} /> Running...</> : '▶  Start Full Pipeline'}
            </Btn>
          </CardBody>
        </Card>

        {/* Progress + log */}
        <Card>
          <CardHeader icon="📋" iconColor="green" title="Live progress">
            <Btn size="sm" onClick={() => setLogs([])}>Clear</Btn>
          </CardHeader>
          <CardBody>
            <PipelineTrack stages={stages} />

            {/* Log */}
            <div ref={logRef} style={{
              background: '#15140f', borderRadius: 'var(--radius-sm)',
              padding: '12px 14px', minHeight: 140, maxHeight: 200,
              overflowY: 'auto', fontFamily: "'JetBrains Mono', monospace",
              fontSize: 11.5, lineHeight: 1.9,
            }}>
              {logs.map((l, i) => (
                <div key={i} style={{ display: 'flex', gap: 10 }}>
                  <span style={{ color: '#444', flexShrink: 0 }}>{l.time}</span>
                  <span style={{ color: logColors[l.cls] || logColors.info }}>{l.msg}</span>
                </div>
              ))}
            </div>

            {/* Results */}
            {results && results.length > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ height: 1, background: 'var(--border)', margin: '0 0 14px' }} />
                <div style={{ fontSize: 13, fontWeight: 700, marginBottom: 10 }}>Results</div>
                <Grid cols={Math.min(results.length, 3)} gap={10}>
                  {results.map((r, i) => {
                    const m = r.metrics || {};
                    return (
                      <div key={i} style={{ background: 'var(--bg)', borderRadius: 'var(--radius-sm)', padding: 12, textAlign: 'center', border: '1px solid var(--border)' }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--text2)', marginBottom: 4 }}>{r.dataset_name || r.dataset_id?.slice(0, 8)}</div>
                        <div style={{ fontSize: 20, fontWeight: 700, color: 'var(--green)' }}>{m.silver?.rows_written || 0}</div>
                        <div style={{ fontSize: 11, color: 'var(--text2)' }}>silver rows</div>
                      </div>
                    );
                  })}
                </Grid>
              </div>
            )}
          </CardBody>
        </Card>
      </Grid>
    </div>
  );
}
