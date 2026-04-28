import React, { useState } from 'react';
import { FiActivity, FiArrowRight, FiCheck, FiCloud, FiCpu, FiDatabase, FiFile, FiSearch, FiSettings, FiZap } from 'react-icons/fi';
import CloudPortalScanModal from './orchestration/CloudPortalScanModal';
import './PipelineIntelligence.css';

const TARGETS = [
  { id: 'aws', label: 'AWS Connected', icon: <FiCloud /> },
  { id: 'azure', label: 'Azure Active (SSO)', icon: <FiCloud /> },
  { id: 'fabric', label: 'Microsoft Fabric', icon: <FiZap /> },
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

export default function PipelineIntelligence({ clientName, initialData, onScanComplete, onConfirm }) {
  const [data, setData] = useState(initialData || null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [target, setTarget] = useState(initialData?.ingestion_details?.target || 'aws');
  const [useCloudLlm, setUseCloudLlm] = useState(true);
  const [showCloudScanModal, setShowCloudScanModal] = useState(false);

  const flow = data?.interactive_flow || data?.loading_flow || [];
  const support = data?.ingestion_support || {};
  const delimiter = data?.delimiter_config || {};
  const capabilities = data?.pipeline_capabilities || {};

  return (
    <div className="pipeline-intelligence-container">
      <div className="pi-header">
        <div>
          <h2 className="pi-title">Pipeline Intelligence</h2>
          <p className="pi-subtitle">Discover pipeline architecture, ingestion support, configuration, and DQ signals for {clientName}.</p>
        </div>
      </div>

      <div className="pi-target-grid">
        {TARGETS.map((item) => (
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
      </div>

      <label className="pi-checkbox-row">
        <input type="checkbox" checked={useCloudLlm} onChange={(e) => setUseCloudLlm(e.target.checked)} />
        <span>Use GPT API to extract ingestion, source, and DQ rules</span>
      </label>

      <div className="pi-scan-trigger">
        <button
          className="pi-btn-confirm"
          onClick={() => {
            if (!clientName) {
              setError('Missing client selection.');
              return;
            }
            setError(null);
            setShowCloudScanModal(true);
          }}
          disabled={loading || !clientName}
        >
          <FiSearch /> Scan Framework
        </button>
      </div>

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
                {data.is_fallback ? ' · Fallback response' : ''}
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
              <JsonBlock value={data.original_config || {}} />
            </div>
            <div className="pi-card pi-wide">
              <div className="pi-card-title">Reformatted Config JSON</div>
              <JsonBlock value={data.reformatted_config || {}} />
            </div>
          </div>

          <div className="pi-actions">
            <button className="pi-btn-confirm" onClick={() => onConfirm(data)}>
              <FiCheck /> Review Data Sources
            </button>
          </div>
        </>
      )}

      {showCloudScanModal && (
        <CloudPortalScanModal
          selectedClient={clientName}
          initialTarget={target}
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
