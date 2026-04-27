import { useState } from 'react';
import { apiCall } from '../hooks/useApi';
import { useToast } from '../hooks/useToast';
import { Card, CardHeader, CardBody, Btn, Input, Select, Badge, Table } from '../components/UI';

export default function Browse() {
  const [src, setSrc] = useState('ADLS');
  const [client, setClient] = useState('');
  const [folder, setFolder] = useState('');
  const [rows, setRows] = useState([]);
  const [count, setCount] = useState('0 items');
  const toast = useToast();

  const listFiles = async () => {
    if (!client.trim()) { toast('Enter a client name', 'error'); return; }
    try {
      const r = await apiCall(`/connect/list?source_type=${src}&client_name=${encodeURIComponent(client)}&folder_path=${encodeURIComponent(folder)}`);
      const list = Array.isArray(r) ? r : [];
      if (r.detail) { toast(r.detail, 'error'); return; }
      setCount(list.length + ' files');
      setRows(list.map(f => [
        <span style={{ fontWeight: 600 }}>📄 {f.file_name}</span>,
        <Badge variant="blue">{f.file_format}</Badge>,
        f.file_size ? (f.file_size / 1024).toFixed(1) + ' KB' : '—',
        <Badge variant={f.source_type === 'API' ? 'green' : 'purple'}>{f.source_type}</Badge>,
        <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, color: 'var(--text2)' }}>{f.file_path}</span>,
      ]));
    } catch (e) { toast('Error: ' + e.message, 'error'); }
  };

  const browse = async () => {
    if (!client.trim()) { toast('Enter a client name', 'error'); return; }
    try {
      const r = await apiCall(`/connect/browse?source_type=${src}&client_name=${encodeURIComponent(client)}&path=${encodeURIComponent(folder)}`);
      if (r.detail) { toast(r.detail, 'error'); return; }
      const folders = r.folders || [], files = r.files || [];
      setCount((folders.length + files.length) + ' items');
      setRows([
        ...folders.map(f => [
          <span style={{ fontWeight: 600 }}>📁 {f}/</span>,
          'Folder', '—', '—', '—',
        ]),
        ...files.map(f => [
          <span style={{ fontWeight: 600 }}>📄 {f.file_name}</span>,
          <Badge variant="blue">{f.file_format}</Badge>,
          f.file_size ? (f.file_size / 1024).toFixed(1) + ' KB' : '—',
          f.source_type,
          <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, color: 'var(--text2)' }}>{f.file_path}</span>,
        ]),
      ]);
    } catch (e) { toast('Error: ' + e.message, 'error'); }
  };

  return (
    <div className="animate-in">
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: 24, fontWeight: 700, marginBottom: 3 }}>🔍 Browse & Connect</div>
        <div style={{ fontSize: 13.5, color: 'var(--text2)' }}>Explore available files or API data before ingesting.</div>
      </div>

      <Card>
        <CardHeader icon="🔍" iconColor="blue" title="Search" />
        <CardBody>
          <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end', flexWrap: 'wrap' }}>
            <div style={{ minWidth: 140 }}>
              <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>Source type</label>
              <Select value={src} onChange={e => setSrc(e.target.value)}>
                <option value="ADLS">☁️ ADLS</option>
                <option value="API">🔌 API</option>
              </Select>
            </div>
            <div style={{ flex: 1 }}>
              <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>Client name</label>
              <Input value={client} onChange={e => setClient(e.target.value)} placeholder="e.g. AMGEN" />
            </div>
            <div style={{ flex: 1 }}>
              <label style={{ display: 'block', fontSize: 12.5, fontWeight: 700, marginBottom: 5 }}>Folder / endpoint</label>
              <Input value={folder} onChange={e => setFolder(e.target.value)} placeholder="e.g. clinical" />
            </div>
            <Btn variant="primary" onClick={listFiles}>List Files</Btn>
            <Btn onClick={browse}>Browse Folders</Btn>
          </div>
        </CardBody>
      </Card>

      <Card>
        <CardHeader icon="📂" iconColor="amber" title="Results">
          <Badge variant="gray">{count}</Badge>
        </CardHeader>
        <CardBody>
          <Table
            headers={['File name', 'Format', 'Size', 'Source', 'Path']}
            rows={rows}
            emptyMsg="Run a search above to see files."
          />
        </CardBody>
      </Card>
    </div>
  );
}
