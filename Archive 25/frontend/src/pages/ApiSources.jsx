import { useState, useEffect } from 'react';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Grid, FormGroup, Input, Select, Badge, InfoBox } from '../components/UI';

const EXAMPLES = {
  cdc:  { client: 'CDC',   name: 'disease-api',     url: 'https://disease.sh/v3/covid-19',        auth: 'none', ep: 'countries,continents,all' },
  fda:  { client: 'AMGEN', name: 'fda-api',         url: 'https://api.fda.gov/drug',              auth: 'none', ep: 'event.json,label.json' },
  test: { client: 'TEST',  name: 'jsonplaceholder', url: 'https://jsonplaceholder.typicode.com',  auth: 'none', ep: 'users,posts,todos' },
};

export default function ApiSources() {
  const [sources, setSources] = useState([]);
  const [form, setForm] = useState({ client: '', name: '', url: '', auth: 'none', token: '', header: 'X-Api-Key', endpoints: '' });
  const [testing, setTesting] = useState(false);
  const [verified, setVerified] = useState(false);
  const toast = useToast();

  const load = async () => {
    try { const r = await apiCall('/api-source/list'); setSources(r.configs || []); } catch {}
  };

  useEffect(() => { load(); }, []);

  const fill = (key) => {
    const e = EXAMPLES[key];
    setForm(f => ({ ...f, client: e.client, name: e.name, url: e.url, auth: e.auth, endpoints: e.ep, token: '' }));
    setVerified(false);
  };

  const set = (k, v) => {
    setForm(f => ({ ...f, [k]: v }));
    setVerified(false);
  };

  const testConnection = async () => {
    if (!form.client.trim() || !form.url.trim()) { toast('Client name and Base URL required', 'error'); return; }
    setTesting(true);
    setVerified(false);
    try {
      const body = {
        client_name: form.client.trim(), source_name: form.name.trim(),
        base_url: form.url.trim(), auth_type: form.auth,
        auth_token: form.token.trim() || null,
        api_key_header: form.header.trim() || 'X-Api-Key',
        endpoints: form.endpoints.trim(),
        source_type: 'API'
      };
      const r = await apiCall('/api-source/test-connection', 'POST', body);
      if (r.status === 'SUCCESS') {
        toast(r.message, 'success');
        setVerified(true);
      } else {
        toast('Connection failed: ' + r.message, 'error');
      }
    } catch (e) {
      toast('Test failed: ' + e.message, 'error');
    } finally {
      setTesting(false);
    }
  };

  const register = async () => {
    if (!form.client.trim() || !form.url.trim()) { toast('Client name and Base URL required', 'error'); return; }
    const body = {
      client_name: form.client.trim(), source_name: form.name.trim(),
      base_url: form.url.trim(), auth_type: form.auth,
      auth_token: form.token.trim() || null,
      api_key_header: form.header.trim() || 'X-Api-Key',
      endpoints: form.endpoints.trim(),
    };
    try {
      const r = await apiCall('/api-source/register', 'POST', body);
      if (r.detail) { toast(r.detail, 'error'); return; }
      toast('API source registered for ' + body.client_name + ' ✓', 'success');
      setForm({ client: '', name: '', url: '', auth: 'none', token: '', header: 'X-Api-Key', endpoints: '' });
      load();
    } catch (e) { toast('Error: ' + e.message, 'error'); }
  };

  const del = async (id) => {
    if (!window.confirm('Delete this API source?')) return;
    await apiCall(`/api-source/${id}`, 'DELETE');
    toast('Deleted', 'success');
    load();
  };

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>🔌 API Sources</div>
        <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Connect any REST API as a data source — register once, ingest anytime.</div>
      </div>

      <Grid cols={2} gap={18}>
        {/* Register form */}
        <Card>
          <CardHeader icon="➕" iconColor="green" title="Register a new API" />
          <CardBody>
            <InfoBox variant="tip">
              💡 Quick fill: &nbsp;
              {Object.keys(EXAMPLES).map(k => (
                <Btn key={k} size="sm" style={{ marginRight: 4 }} onClick={() => fill(k)}>
                  {k === 'cdc' ? 'CDC' : k === 'fda' ? 'FDA' : 'Test API'}
                </Btn>
              ))}
            </InfoBox>

            <FormGroup label="Client name"><Input value={form.client} onChange={e => set('client', e.target.value)} placeholder="e.g. CDC" /></FormGroup>
            <FormGroup label="Source name" hint="A short label for this connection"><Input value={form.name} onChange={e => set('name', e.target.value)} placeholder="e.g. disease-api" /></FormGroup>
            <FormGroup label="Base URL"><Input value={form.url} onChange={e => set('url', e.target.value)} placeholder="https://disease.sh/v3/covid-19" /></FormGroup>

            <FormGroup label="Authentication">
              <Select value={form.auth} onChange={e => set('auth', e.target.value)}>
                <option value="none">🔓 No auth (public API)</option>
                <option value="bearer">🔑 Bearer token</option>
                <option value="apikey">🗝️ API key header</option>
                <option value="basic">🔐 Basic auth</option>
              </Select>
            </FormGroup>

            {form.auth !== 'none' && (
              <FormGroup label="Token / Key value">
                <Input type="password" value={form.token} onChange={e => set('token', e.target.value)} placeholder="Paste your token here" />
              </FormGroup>
            )}
            {form.auth === 'apikey' && (
              <FormGroup label="Header name">
                <Input value={form.header} onChange={e => set('header', e.target.value)} placeholder="X-Api-Key" />
              </FormGroup>
            )}

            <FormGroup label="Endpoints (comma-separated)" hint="These become the folder_path options when running orchestration">
              <Input value={form.endpoints} onChange={e => set('endpoints', e.target.value)} placeholder="countries,continents,users" />
            </FormGroup>

            <div style={{ display: 'flex', gap: 10, marginTop: 10 }}>
              <Btn variant="ghost" style={{ flex: 1 }} onClick={testConnection} disabled={testing}>
                {testing ? 'Testing...' : '⚡ Test Connection'}
              </Btn>
              <Btn variant="green" style={{ flex: 1.5 }} onClick={register} disabled={!verified}>
                🔌  Register API Source
              </Btn>
            </div>
          </CardBody>
        </Card>

        {/* List */}
        <Card>
          <CardHeader icon="📋" iconColor="blue" title="Registered APIs">
            <Btn size="sm" onClick={load}>↻ Refresh</Btn>
          </CardHeader>
          <CardBody>
            {sources.length === 0
              ? <div style={{ color: 'var(--text2)', fontSize: 13.5 }}>No API sources registered yet. Use the form on the left.</div>
              : sources.map(s => (
                <div key={s.id} style={{
                  border: '1px solid var(--border)', borderRadius: 'var(--radius-sm)',
                  padding: 14, marginBottom: 10, background: 'var(--bg)',
                }}>
                  <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: 10 }}>
                    <div style={{ flex: 1 }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                        <Badge variant="blue">{s.client_name}</Badge>
                        <span style={{ fontWeight: 700, fontSize: 13.5 }}>{s.source_name}</span>
                        <Badge variant={s.auth_type === 'none' ? 'green' : 'amber'}>
                          {s.auth_type === 'none' ? '🔓 Public' : '🔑 Auth'}
                        </Badge>
                      </div>
                      <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: 'var(--text2)', marginBottom: 6 }}>{s.base_url}</div>
                      <div style={{ fontSize: 12, color: 'var(--text2)' }}>
                        Endpoints: <strong>{(s.endpoints || []).join(', ') || 'none'}</strong>
                      </div>
                    </div>
                    <Btn size="sm" variant="red" onClick={() => del(s.id)}>Delete</Btn>
                  </div>
                </div>
              ))
            }
          </CardBody>
        </Card>
      </Grid>
    </div>
  );
}
