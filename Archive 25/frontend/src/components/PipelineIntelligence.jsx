import React, { useEffect, useMemo, useState } from 'react';
import { 
  FiActivity, FiArrowRight, FiCheck, FiCloud, FiCpu, FiDatabase, 
  FiFile, FiFolder, FiLink, FiSearch, FiSettings, FiZap, 
  FiRefreshCw, FiCopy, FiEdit2, FiPlus, FiAlertCircle 
} from 'react-icons/fi';
import CloudPortalScanModal from './orchestration/CloudPortalScanModal';
import { apiUrl } from '../hooks/useApi';
import './PipelineIntelligence.css';

const STRATEGIES = [
  { id: 'REUSE', label: 'Reuse Existing', icon: <FiZap />, desc: 'Use orchestration as-is, updating only metadata and parameters.' },
  { id: 'CLONE', label: 'Clone Pipeline', icon: <FiCopy />, desc: 'Duplicate the pipeline within the workspace for this execution.' },
  { id: 'MODIFY', label: 'Modify Template', icon: <FiEdit2 />, desc: 'Patch the pipeline definition with custom activities or flow changes.' },
  { id: 'CREATE_NEW', label: 'Create New', icon: <FiPlus />, desc: 'Deploy a completely new pipeline item from an external package.' },
];

const TARGETS = [
  { id: 'aws', sourceType: 'AWS', label: 'AWS Platform', icon: <FiCloud />, scan: true },
  { id: 'azure', sourceType: 'AZURE', label: 'Azure Platform', icon: <FiCloud />, scan: true },
  { id: 'fabric', sourceType: 'FABRIC', label: 'Microsoft Fabric', icon: <FiZap />, scan: true },
  { id: 's3', sourceType: 'S3', label: 'Amazon S3', icon: <FiDatabase />, scan: true },
  { id: 'adls', sourceType: 'ADLS', label: 'Azure Data Lake', icon: <FiDatabase />, scan: true },
  { id: 'api', sourceType: 'REST_API', label: 'REST API', icon: <FiLink />, scan: false },
  { id: 'local', sourceType: 'LOCAL', label: 'Local Files', icon: <FiFolder />, scan: false },
];

function JsonBlock({ value }) {
  return (
    <pre className="pi-json">
      {JSON.stringify(value || {}, null, 2)}
    </pre>
  );
}

function Tag({ active, children }) {
  return <span className={`pi-tag ${active === false ? 'inactive' : 'active'}`}>{children}</span>;
}

function hasApiScanDetails(apiSources = []) {
  return (apiSources || []).some((source) => {
    const endpoints = Array.isArray(source.endpoints)
      ? source.endpoints
      : String(source.endpoints || '').split(',').map((item) => item.trim()).filter(Boolean);
    return !!source.base_url && endpoints.length > 0;
  });
}

export default function PipelineIntelligence({
  clientName,
  initialData,
  clientSourceTypes = [],
  currentSourceType = '',
  apiSources = [],
  fabricDiscoveryData = null,
  fabricMode = 'DISCOVERY',
  selectedPlatform = '',
  selectedWorkspace = null,
  setSelectedWorkspace = () => {},
  selectedPipeline = null,
  setSelectedPipeline = () => {},
  onScanComplete,
  onConfirm
}) {
  const [data, setData] = useState(initialData || null);
  const [loading, setLoading] = useState(false);
  const [scanInProgress, setScanInProgress] = useState(false);
  const [analyzing, setAnalyzing] = useState(false);
  const [error, setError] = useState(null);
  const [target, setTarget] = useState(initialData?.ingestion_details?.target || 'aws');
  const [useCloudLlm, setUseCloudLlm] = useState(true);
  const [showCloudScanModal, setShowCloudScanModal] = useState(false);
  const [scanResults, setScanResults] = useState(null);
  const [deploymentStrategy, setDeploymentStrategy] = useState(null);

  const configuredSourceTypes = useMemo(() => {
    const values = (clientSourceTypes || []).map((item) => String(item || '').toUpperCase()).filter(Boolean);
    const current = String(currentSourceType || '').toUpperCase();
    const mapped = current === 'API' ? 'REST_API' : current;
    if (mapped) values.push(mapped);
    if (selectedPlatform === 'FABRIC' && !values.includes('FABRIC')) values.push('FABRIC');
    return [...new Set(values)];
  }, [clientSourceTypes, currentSourceType, selectedPlatform]);

  const allowedTargets = useMemo(() => TARGETS.filter((item) => configuredSourceTypes.includes(item.sourceType)), [configuredSourceTypes]);
  const selectedTarget = allowedTargets.find((item) => item.id === target);
  const apiDetailsAvailable = hasApiScanDetails(apiSources);
  const selectedRequiresScan = selectedTarget?.sourceType ? (['AWS', 'AZURE', 'FABRIC', 'S3', 'ADLS'].includes(selectedTarget.sourceType) || (selectedTarget.sourceType === 'REST_API' && apiDetailsAvailable)) : false;

  useEffect(() => {
    if (initialData) setData(initialData);
  }, [initialData]);

  useEffect(() => {
    if (allowedTargets.length > 0 && !allowedTargets.some(t => t.id === target)) {
      setTarget(allowedTargets[0].id);
    }
  }, [allowedTargets, target]);

  const handleAnalyzePipeline = async (workspace, pipeline) => {
    if (scanInProgress || analyzing) return;
    setAnalyzing(true);
    setError(null);
    setSelectedWorkspace(workspace);
    setSelectedPipeline(pipeline);

    try {
      const response = await fetch(apiUrl('/discovery/analyze'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          client_name: clientName,
          platform: 'FABRIC',
          source_type: 'FABRIC',
          payload: { workspace_id: workspace.id, pipeline_id: pipeline.id },
          use_cloud_llm: useCloudLlm
        }),
      });
      if (!response.ok) throw new Error('Analysis failed');
      const result = await response.json();
      const finalResult = { ...result, scan_status: 'success', scan_completed: true };
      setData(finalResult);
      onScanComplete?.(finalResult);
    } catch (e) {
      setError("Pipeline analysis failed. Could not extract intelligence metadata.");
    } finally {
      setAnalyzing(false);
    }
  };

  const handleManualApiScan = async () => {
    if (scanInProgress) return;
    setScanInProgress(true);
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(apiUrl('/discovery/api-scan'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ client_name: clientName }),
      });
      if (!response.ok) throw new Error('API scan failed');
      const result = await response.json();
      setData(result);
      onScanComplete?.(result);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
      setScanInProgress(false);
    }
  };

  const flow = data?.interactive_flow || data?.loading_flow || [];
  const support = data?.ingestion_support || {};
  const delimiter = data?.delimiter_config || {};
  const capabilities = data?.pipeline_capabilities || {};

  return (
    <div className="pipeline-intelligence-container">
      <div className="pi-header">
        <h2>Pipeline Intelligence</h2>
        <p className="step-sub">Discover pipeline architecture, ingestion support, configuration, and DQ signals.</p>
      </div>

      <div className="pi-target-grid">
        {allowedTargets.map((item) => (
          <button
            key={item.id}
            className={`pi-target-card ${target === item.id ? 'selected' : ''}`}
            onClick={() => { setTarget(item.id); setData(null); setScanResults(null); }}
            disabled={loading || scanInProgress || analyzing}
          >
            <span className="pi-target-icon">{item.icon}</span>
            <span>{item.label}</span>
          </button>
        ))}
      </div>

      <div className="pi-scan-trigger">
        {selectedTarget?.sourceType !== 'LOCAL' && (
          <label className="pi-checkbox-row" style={{ marginBottom: 20 }}>
            <input type="checkbox" checked={useCloudLlm} onChange={(e) => setUseCloudLlm(e.target.checked)} disabled={loading || scanInProgress || analyzing} />
            <span>Use GPT API to extract ingestion, source, and DQ rules</span>
          </label>
        )}

        {selectedTarget?.sourceType !== 'REST_API' && (
          <button
            className="pi-btn-confirm"
            onClick={() => setShowCloudScanModal(true)}
            disabled={loading || scanInProgress || analyzing || !selectedRequiresScan}
          >
            <FiSearch /> Scan Framework
          </button>
        )}
        
        {selectedTarget?.sourceType === 'REST_API' && apiDetailsAvailable && (
          <button className="pi-btn-confirm" onClick={handleManualApiScan} disabled={loading || scanInProgress || analyzing}>
            <FiSearch /> Scan REST API
          </button>
        )}
      </div>

      {/* FABRIC ASSET EXPLORER */}
      {selectedPlatform === 'FABRIC' && scanResults && !data && !loading && !analyzing && (
        <div className="fabric-explorer-section" style={{ marginTop: 30, padding: 20, background: '#f8fafc', borderRadius: 16, border: '1px solid #e2e8f0' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
            <h3 style={{ fontSize: 18, fontWeight: 900, display: 'flex', alignItems: 'center', gap: 10, margin: 0 }}>
              <FiFolder color="#2563eb" /> Discovered Fabric Workspaces
            </h3>
            <span style={{ fontSize: 12, color: '#64748b', fontWeight: 600 }}>{scanResults.length} Workspaces Found</span>
          </div>

          {scanResults.length > 0 ? (
            <div className="workspace-list" style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
              {scanResults.map(ws => (
                <div key={ws.id} className="workspace-card pi-card pi-wide" style={{ background: '#fff', border: '1px solid #e2e8f0', padding: 0, overflow: 'hidden', borderRadius: 12, boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.05)' }}>
                  <div style={{ background: '#fff', padding: '16px 20px', borderBottom: '1px solid #f1f5f9', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                      <div style={{ padding: 8, background: '#eff6ff', borderRadius: 8 }}><FiDatabase color="#2563eb" size={18} /></div>
                      <div>
                        <div style={{ fontWeight: 800, fontSize: 15, color: '#1e293b' }}>{ws.name || ws.displayName}</div>
                        <div style={{ fontSize: 10, color: '#94a3b8', fontFamily: 'monospace' }}>ID: {ws.id}</div>
                      </div>
                    </div>
                    <div className="pipeline-count-tag" style={{ padding: '4px 10px', background: '#f1f5f9', borderRadius: 20, fontSize: 11, fontWeight: 700, color: '#64748b' }}>
                      {(ws.pipelines || ws.data_pipelines || []).length} Pipelines
                    </div>
                  </div>
                  <div className="pipeline-list" style={{ padding: 15, display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: 10, background: '#fafafa' }}>
                    {(ws.pipelines || ws.data_pipelines || []).map(pl => (
                      <div key={pl.id} style={{ padding: '12px 16px', background: '#fff', border: '1px solid #e2e8f0', borderRadius: 10, display: 'flex', justifyContent: 'space-between', alignItems: 'center', transition: 'all 0.2s ease' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                          <FiActivity color="#6366f1" size={16} />
                          <div style={{ fontSize: 13, fontWeight: 700, color: '#334155' }}>{pl.name || pl.displayName}</div>
                        </div>
                        <button 
                          className="orch-btn primary tiny" 
                          onClick={() => handleAnalyzePipeline(ws, pl)}
                          style={{ fontSize: 11, padding: '6px 14px', borderRadius: 8, background: '#2563eb', color: '#fff', border: 'none', cursor: 'pointer', fontWeight: 600 }}
                        >
                          Analyze Pipeline
                        </button>
                      </div>
                    ))}
                    {!(ws.pipelines || ws.data_pipelines || []).length && (
                      <div style={{ gridColumn: '1/-1', textAlign: 'center', padding: '20px', color: '#94a3b8', fontSize: 13, fontStyle: 'italic' }}>
                        No pipelines discovered in this workspace.
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div style={{ textAlign: 'center', padding: '40px 20px', background: '#fff', borderRadius: 12, border: '1px dashed #cbd5e1' }}>
              <FiSearch size={40} color="#94a3b8" style={{ marginBottom: 16 }} />
              <div style={{ fontWeight: 700, fontSize: 16, color: '#475569' }}>No Workspaces Discovered</div>
              <p style={{ color: '#94a3b8', fontSize: 14, margin: '8px 0 0' }}>The scan completed but no accessible Fabric workspaces were found.</p>
            </div>
          )}
        </div>
      )}

      {analyzing && (
        <div className="pi-loading" style={{ marginTop: 30 }}>
          <div className="pi-spinner" />
          <p>Extracting deep intelligence from <strong>{selectedPipeline?.name}</strong>...</p>
        </div>
      )}

      {loading && (
        <div className="pi-loading">
          <div className="pi-spinner" />
          <p>Scanning live environment...</p>
        </div>
      )}

      {error && <div className="pi-error"><FiAlertCircle /> {error}</div>}

      {/* INTELLIGENCE & STRATEGY */}
      {data && !analyzing && !loading && (
        <>
          {selectedPlatform === 'FABRIC' && (
            <div className="pi-card pi-wide" style={{ border: '1px solid #3b82f6', background: 'rgba(59, 130, 246, 0.05)', marginTop: 24 }}>
              <div className="pi-card-title" style={{ color: '#2563eb' }}><FiSettings /> PIPELINE REUSE STRATEGY</div>
              <div className="pi-strategy-grid">
                {STRATEGIES.map((s) => (
                  <button key={s.id} className={`pi-strategy-card ${deploymentStrategy === s.id ? 'selected' : ''}`} onClick={() => setDeploymentStrategy(s.id)}>
                    <div className="pi-strategy-icon">{s.icon}</div>
                    <div className="pi-strategy-info">
                      <div className="pi-strategy-label">{s.label}</div>
                      <div className="pi-strategy-desc">{s.desc}</div>
                    </div>
                  </button>
                ))}
              </div>
            </div>
          )}

          <div className="pi-grid">
            <div className="pi-card"><div className="pi-card-title"><FiCpu /> Detected Framework</div><div className="pi-card-content pi-framework">{data.framework || 'Unknown'}</div></div>
            <div className="pi-card"><div className="pi-card-title"><FiDatabase /> Ingestion Support</div><div className="pi-tag-list"><Tag active={support.file_based}>File-based</Tag><Tag active={support.api}>API</Tag><Tag active={support.database}>Database</Tag></div></div>
            <div className="pi-card"><div className="pi-card-title"><FiSettings /> Delimiters</div><div className="pi-card-content">{delimiter.column_delimiter || ','} | {delimiter.quote_char || '"'}</div></div>
            <div className="pi-card"><div className="pi-card-title"><FiZap /> Capabilities</div><div className="pi-tag-list">{Object.entries(capabilities).map(([k, v]) => <Tag key={k} active={!!v}>{k}</Tag>)}</div></div>
            <div className="pi-card pi-wide"><div className="pi-card-title"><FiActivity /> Interactive Flow</div><div className="pi-flow-viz">{flow.map((n, i) => <span key={i}>{n.label} {i < flow.length - 1 && '→'} </span>)}</div></div>
            <div className="pi-card"><div className="pi-card-title">Cloud Scan</div><JsonBlock value={data.raw_cloud_scan || {}} /></div>
            <div className="pi-card"><div className="pi-card-title">Reformatted Config</div><JsonBlock value={data.reformatted_config || {}} /></div>
          </div>

          <div className="pi-actions">
            <button className="pi-btn-confirm" onClick={() => onConfirm({ ...data, deploymentStrategy, selectedWorkspace, selectedPipeline })} disabled={selectedPlatform === 'FABRIC' && !deploymentStrategy}>
              <FiCheck /> {selectedPlatform === 'FABRIC' ? 'Confirm Strategy & Configure' : 'Configure Data Sources'}
            </button>
          </div>
        </>
      )}

      {showCloudScanModal && (
        <CloudPortalScanModal
          selectedClient={clientName}
          initialTarget={target}
          allowedTargets={allowedTargets.filter((item) => item.scan).map((item) => item.id)}
          sourceType={selectedTarget?.sourceType}
          useCloudLlm={useCloudLlm}
          onTargetChange={setTarget}
          onClose={() => setShowCloudScanModal(false)}
          onScanComplete={(result) => {
            setShowCloudScanModal(false);
            if (selectedPlatform === 'FABRIC') {
               const discovered = result.fabric_workspaces || 
                                  result.workspaces || 
                                  result.raw_cloud_scan?.fabric_workspaces || 
                                  result.raw_cloud_scan?.workspaces || 
                                  result.payload?.fabric_workspaces || [];
               setScanResults(discovered);
               setData(null);
               setSelectedPipeline(null);
            } else {
               setData(result);
               onScanComplete?.(result);
            }
          }}
        />
      )}
    </div>
  );
}
