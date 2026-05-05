import React, { useEffect, useMemo, useState } from 'react';
import { FiActivity, FiArrowRight, FiCheck, FiCloud, FiCpu, FiDatabase, FiFile, FiFolder, FiLink, FiSearch, FiSettings, FiZap } from 'react-icons/fi';
import CloudPortalScanModal from './orchestration/CloudPortalScanModal';
import { apiUrl } from '../hooks/useApi';
import './PipelineIntelligence.css';

const TARGETS = [
  { id: 'aws', sourceType: 'AWS', label: 'AWS Connected', icon: <FiCloud />, scan: true },
  { id: 'azure', sourceType: 'AZURE', label: 'Azure Active (SSO)', icon: <FiCloud />, scan: true },
  { id: 'fabric', sourceType: 'FABRIC', label: 'Microsoft Fabric', icon: <FiZap />, scan: true },
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

function hasJsonValue(value) {
  if (!value) return false;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === 'object') return Object.keys(value).length > 0;
  return true;
}

function Tag({ active, children }) {
  return <span className={`pi-tag ${active === false ? 'inactive' : 'active'}`}>{children}</span>;
}

function AlertList({ title, items, tone = 'warning' }) {
  if (!items || items.length === 0) return null;
  return (
    <div className={`pi-alert ${tone}`}>
      <strong>{title}</strong>
      <ul>
        {items.map((item, idx) => <li key={`${title}-${idx}`}>{item}</li>)}
      </ul>
    </div>
  );
}

function hasApiScanDetails(apiSources = []) {
  return (apiSources || []).some((source) => {
    const endpoints = Array.isArray(source.endpoints)
      ? source.endpoints
      : String(source.endpoints || '').split(',').map((item) => item.trim()).filter(Boolean);
    return !!source.base_url && endpoints.length > 0;
  });
}

export default function PipelineIntelligence({ clientName, initialData, clientSourceTypes = [], currentSourceType = '', apiSources = [], fabricDiscoveryData = null, onScanComplete, onConfirm }) {
  const [data, setData] = useState(initialData || null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [target, setTarget] = useState(initialData?.ingestion_details?.target || 'aws');
  const [useCloudLlm, setUseCloudLlm] = useState(true);
  const [showCloudScanModal, setShowCloudScanModal] = useState(false);
  const configuredSourceTypes = useMemo(
    () => {
      const values = (clientSourceTypes || []).map((item) => String(item || '').toUpperCase()).filter(Boolean);
      const current = String(currentSourceType || '').toUpperCase();
      const mapped = current === 'S3' ? 'AWS' : current === 'ADLS' ? 'AZURE' : current === 'API' ? 'REST_API' : current;
      if (mapped) values.push(mapped);
      return [...new Set(values)];
    },
    [clientSourceTypes, currentSourceType]
  );
  const allowedTargets = useMemo(
    () => TARGETS.filter((item) => configuredSourceTypes.includes(item.sourceType)),
    [configuredSourceTypes]
  );
  const selectedTarget = allowedTargets.find((item) => item.id === target);
  const apiDetailsAvailable = hasApiScanDetails(apiSources);
  const selectedRequiresScan = selectedTarget?.sourceType
    ? (['AWS', 'AZURE', 'FABRIC'].includes(selectedTarget.sourceType) || (selectedTarget.sourceType === 'REST_API' && apiDetailsAvailable))
    : false;
  const selectedMessage = (() => {
    if (selectedTarget?.sourceType === 'LOCAL') return 'Local File Mode: No scan required. Proceed to upload files.';
    if (selectedTarget?.sourceType === 'REST_API' && !apiDetailsAvailable) return 'Provide API details to enable scanning';
    return '';
  })();

  const flow = data?.interactive_flow || data?.loading_flow || [];
  const support = data?.ingestion_support || {};
  const delimiter = data?.delimiter_config || {};
  const capabilities = data?.pipeline_capabilities || {};

  useEffect(() => {
    setData(initialData || null);
    setError(null);
    setLoading(false);
  }, [clientName, initialData]);

  useEffect(() => {
    if (allowedTargets.length === 0) {
      setTarget('');
      return;
    }
    if (!allowedTargets.some((item) => item.id === target)) {
      setTarget(allowedTargets[0].id);
    }
  }, [allowedTargets, target]);

  useEffect(() => {
    if (currentSourceType === 'FABRIC' && fabricDiscoveryData && !data && !loading) {
        // Automatically run analysis for discovered fabric pipeline
        runFabricAnalysis();
    }
  }, [currentSourceType, fabricDiscoveryData]);

  const runFabricAnalysis = async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(apiUrl('/fabric/analyze'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
            client_name: clientName,
            pipeline_json: fabricDiscoveryData.pipeline || fabricDiscoveryData
        }),
      });
      if (!response.ok) throw new Error('Fabric analysis failed');
      const result = await response.json();
      setData(result);
      onScanComplete?.(result);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="pipeline-intelligence-container">
      <div className="pi-header">
        <div>
          <h2 className="pi-title">Pipeline Intelligence</h2>
          <p className="pi-subtitle">Discover pipeline architecture, ingestion support, configuration, and DQ signals for {clientName}.</p>
        </div>
      </div>

      <div className="pi-target-grid">
        {allowedTargets.map((item) => (
          <button
            key={item.id}
            className={`pi-target-card ${target === item.id ? 'selected' : ''}`}
            onClick={() => setTarget(item.id)}
            type="button"
          >
            <span className="pi-target-icon">{item.icon}</span>
            <span>{item.label}</span>
          </button>
        ))}
        {allowedTargets.length === 0 && (
          <div className="pi-card pi-wide">
            <div className="pi-card-title">No Source Type Configured</div>
            <div className="pi-card-content">Register or upload a source for this client before running Pipeline Intelligence.</div>
          </div>
        )}
      </div>

      {selectedMessage && (
        <div className="pi-card pi-wide">
          <div className="pi-card-content">{selectedMessage}</div>
        </div>
      )}

      {selectedTarget?.sourceType !== 'LOCAL' && (
        <label className="pi-checkbox-row">
          <input type="checkbox" checked={useCloudLlm} onChange={(e) => setUseCloudLlm(e.target.checked)} />
          <span>Use GPT API to extract ingestion, source, and DQ rules</span>
        </label>
      )}

      <div className="pi-scan-trigger">
        {selectedTarget?.sourceType !== 'REST_API' && (
        <button
          className="pi-btn-confirm"
          onClick={() => {
            if (!clientName) {
              setError('Missing client selection.');
              return;
            }
            if (!selectedTarget) {
              setError('No source type is configured for this client.');
              return;
            }
            if (!selectedRequiresScan) {
              setError(`${selectedTarget.label} does not require a framework scan. Continue to Data Sources and use the registered/manual source.`);
              return;
            }
            setError(null);
            setShowCloudScanModal(true);
          }}
          disabled={loading || !clientName || !selectedRequiresScan}
        >
          <FiSearch /> Scan Framework
        </button>
        )}
        {selectedTarget?.sourceType === 'REST_API' && apiDetailsAvailable && (
          <button
            className="pi-btn-confirm"
            onClick={async () => {
              setLoading(true);
              setError(null);
              try {
                const response = await fetch(apiUrl('/discovery/api-scan'), {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify({ client_name: clientName }),
                });
                if (!response.ok) {
                  let message = `API scan failed with status ${response.status}`;
                  try {
                    const failure = await response.json();
                    message = failure.detail || failure.message || message;
                  } catch {}
                  throw new Error(message);
                }
                const result = await response.json();
                setData(result);
                onScanComplete?.(result);
              } catch (scanError) {
                setError(scanError?.message || 'REST API scan failed.');
              } finally {
                setLoading(false);
              }
            }}
            disabled={loading || !clientName}
            type="button"
          >
            <FiSearch /> Scan REST API
          </button>
        )}
        {selectedTarget && !selectedRequiresScan && (
          <button className="pi-btn-secondary" onClick={() => onConfirm(null)} type="button">
            Continue to Data Sources
          </button>
        )}
      </div>

      {currentSourceType === 'FABRIC' && !fabricDiscoveryData && (
          <div className="pi-card pi-wide" style={{ marginTop: '20px' }}>
             <div className="pi-card-title"><FiCheck color="#10b981" /> Fabric Deployment Ready</div>
             <div className="pi-card-content">
                Fabric pipeline has been deployed to the target workspace. You can now proceed to review data sources or configure the ingestion.
             </div>
             <div className="pi-actions" style={{ marginTop: '20px' }}>
                <button className="pi-btn-confirm" onClick={() => onConfirm(null)}>Continue to Data Sources</button>
             </div>
          </div>
      )}

      {loading && (
        <div className="pi-loading">
          <div className="pi-spinner"></div>
          <p>Scanning live environment and framework configuration...</p>
          <div className="pi-loader-steps">
            <div><FiActivity className="icon-spin" /> Discovering resources</div>
            <div>Extracting ingestion capabilities</div>
            <div>Preparing DEA configuration</div>
          </div>
        </div>
      )}

      {error && (
        <div className="pi-error">
          <strong>Analysis Error:</strong> {error}
          <div className="pi-actions">
            <button className="pi-btn-secondary" onClick={() => onConfirm(null)}>
              Skip & Continue Manually
            </button>
          </div>
        </div>
      )}

      {data && (
        <>
          <div className="pi-grid">
            <div className="pi-card">
              <div className="pi-card-title"><FiCpu /> Detected Framework</div>
              <div className="pi-card-content pi-framework">{data.framework || 'Unknown'}</div>
              <div className="pi-card-content" style={{ marginTop: 8, fontSize: 12 }}>
                Status: {data.scan_status || 'success'} · Auth: {data.auth_mode || 'none'}
              </div>
              <div style={{ marginTop: 10 }}>
                <span className={`pi-tag ${data.is_fallback ? 'inactive' : 'active'}`}>
                  {data.is_fallback ? 'Demo/Fallback Scan' : 'Real Scan'}
                </span>
              </div>
            </div>

            <div className="pi-card">
              <div className="pi-card-title"><FiDatabase /> Ingestion Support by Framework</div>
              <div className="pi-card-content">
                <div className="pi-tag-list">
                  <Tag active={support.file_based}>File-based</Tag>
                  <Tag active={support.api}>API</Tag>
                  <Tag active={support.database}>Database/Table</Tag>
                  <Tag active={support.streaming}>Streaming</Tag>
                  <Tag active={support.batch}>Batch</Tag>
                </div>
              </div>
            </div>

            <div className="pi-card">
              <div className="pi-card-title"><FiFile /> File Types</div>
              <div className="pi-card-content">
                <div className="pi-tag-list">
                  {(data.file_types || []).map((ft) => <Tag key={ft}>{ft}</Tag>)}
                  {(!data.file_types || data.file_types.length === 0) && <Tag active={false}>None Detected</Tag>}
                </div>
              </div>
            </div>

            <div className="pi-card">
              <div className="pi-card-title"><FiSettings /> Delimiters</div>
              <div className="pi-card-content pi-kv-grid">
                <div><strong>Delimiter:</strong> <span className="pi-tag">{delimiter.column_delimiter || ','}</span></div>
                <div><strong>Quote:</strong> <span className="pi-tag">{delimiter.quote_char || '"'}</span></div>
                <div><strong>Escape:</strong> <span className="pi-tag">{delimiter.escape_char || '\\'}</span></div>
                <div><strong>Header:</strong> <span className="pi-tag">{delimiter.header ? 'true' : 'false'}</span></div>
              </div>
            </div>

            <div className="pi-card">
              <div className="pi-card-title"><FiActivity /> DQ Rules</div>
              <div className="pi-card-content">
                <div className="pi-tag-list">
                  {Object.entries(data.dq_rules || {}).map(([key, value]) => (
                    <Tag key={key} active={!!value}>{key.replace(/_/g, ' ')}</Tag>
                  ))}
                </div>
              </div>
            </div>

            <div className="pi-card">
              <div className="pi-card-title"><FiZap /> Pipeline Capabilities</div>
              <div className="pi-card-content">
                <div className="pi-tag-list">
                  {Object.entries(capabilities).map(([key, value]) => (
                    <Tag key={key} active={!!value}>{key.replace(/_/g, ' ')}</Tag>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <AlertList title="Warnings" items={data.warnings || []} />
          <AlertList title="Errors" items={data.errors || []} tone="error" />

          {data.llm_summary && (
            <div className="pi-card">
              <div className="pi-card-title">GPT Summary</div>
              <div className="pi-card-content">{data.llm_summary}</div>
            </div>
          )}

          <div className="pi-card">
            <div className="pi-card-title"><FiArrowRight /> Interactive Flow</div>
            <div className="pi-card-content">
              <div className="pi-flow">
                {flow.map((step, idx) => (
                  <React.Fragment key={`${step}-${idx}`}>
                    <div className="pi-flow-step">{step}</div>
                    {idx < flow.length - 1 && <div className="pi-flow-arrow"><FiArrowRight /></div>}
                  </React.Fragment>
                ))}
              </div>
            </div>
          </div>

          <div className="pi-section-grid">
            <div className="pi-card">
              <div className="pi-card-title">Source Systems</div>
              <JsonBlock value={data.source_systems || []} />
            </div>
            <div className="pi-card">
              <div className="pi-card-title">Discovered Assets</div>
              <JsonBlock value={data.discovered_assets || []} />
            </div>
            <div className="pi-card">
              <div className="pi-card-title">Data Pipelines</div>
              <JsonBlock value={data.data_pipelines || []} />
            </div>
            <div className="pi-card">
              <div className="pi-card-title">Ingestion Details</div>
              <JsonBlock value={data.ingestion_details || {}} />
            </div>
            <div className="pi-card">
              <div className="pi-card-title">Original Config JSON</div>
              {hasJsonValue(data.original_config) ? (
                <JsonBlock value={data.original_config} />
              ) : (
                <div className="pi-card-content">Original Fabric pipeline JSON not provided or could not be extracted from Fabric API.</div>
              )}
            </div>
            <div className="pi-card">
              <div className="pi-card-title">Raw Cloud Scan JSON</div>
              <JsonBlock value={data.raw_cloud_scan || { raw_cloud_dump: data.raw_cloud_dump || [] }} />
            </div>
            <div className="pi-card pi-wide">
              <div className="pi-card-title">Reformatted Config JSON</div>
              <JsonBlock value={data.reformatted_config || {}} />
            </div>
          </div>

          <div className="pi-actions">
            <button className="pi-btn-confirm" onClick={() => onConfirm(data)} disabled={data.scan_status === 'failed'}>
              <FiCheck /> Review Data Sources
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
          onClose={() => {
            setLoading(false);
            setShowCloudScanModal(false);
          }}
          onScanComplete={(result) => {
            setData(result);
            onScanComplete?.(result);
          }}
        />
      )}
    </div>
  );
}
