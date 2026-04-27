import React, { useState } from 'react';
import { FiCheck, FiCpu, FiDatabase, FiFile, FiSettings, FiActivity, FiArrowRight, FiPlay, FiSearch } from 'react-icons/fi';
import './PipelineIntelligence.css';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "http://127.0.0.1:8001";

export async function executeLiveScan(payload) {
  const response = await fetch(`${API_BASE_URL}/discovery/analyze`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    throw new Error(`Live scan failed: ${response.statusText}`);
  }
  return response.json();
}

export default function PipelineIntelligence({ clientName, onConfirm }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [scanProviders, setScanProviders] = useState('aws,azure');

  const handleScan = async () => {
    if (!clientName) {
      setError("Missing client selection.");
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setData(null);
      
      const result = await executeLiveScan({
        client_name: clientName,
        providers: scanProviders
      });
      
      setData(result);
    } catch (err) {
      setError(err.message || "Failed to scan live environment.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="pipeline-intelligence-container">
      <div className="pi-header">
        <div>
          <h2 className="pi-title">Scan Live Environment</h2>
          <p className="pi-subtitle">Discover pipeline architectures, configurations, and data assets automatically.</p>
        </div>
      </div>

      {!data && !loading && (
        <div className="pi-scan-trigger">
          <div className="pi-scan-options">
            <label><strong>Target Providers (comma separated):</strong></label>
            <input 
              type="text" 
              value={scanProviders} 
              onChange={e => setScanProviders(e.target.value)} 
              placeholder="aws,azure,fabric,gcp"
              style={{ padding: '8px', borderRadius: '4px', border: '1px solid #ccc', marginLeft: '12px' }}
            />
          </div>
          <button className="pi-btn-confirm" onClick={handleScan} style={{ marginTop: '16px', background: '#10b981' }}>
            <FiSearch /> Execute Scan
          </button>
        </div>
      )}

      {loading && (
        <div className="pi-loading">
          <div className="pi-spinner"></div>
          <p>Scanning Live Cloud Environment & Framework Configs...</p>
          <div className="pi-loader-steps" style={{ marginTop: '12px', fontSize: '13px', color: '#666' }}>
            <div><FiActivity className="icon-spin" /> Discovering global resources...</div>
            <div style={{ opacity: 0.7 }}>Pulling network architectures...</div>
            <div style={{ opacity: 0.5 }}>Synthesizing capabilities...</div>
          </div>
        </div>
      )}

      {error && (
        <div className="pipeline-intelligence-container">
          <div style={{ padding: '20px', background: '#fee2e2', color: '#b91c1c', borderRadius: '8px' }}>
            <strong>Analysis Error:</strong> {error}
          </div>
          <div className="pi-actions">
            <button className="pi-btn-confirm" onClick={() => onConfirm(null)}>
              Skip & Continue Manually
            </button>
          </div>
        </div>
      )}

      {data && (
        <>
          <div className="pi-grid">
            {/* Framework */}
            <div className="pi-card">
              <div className="pi-card-title"><FiCpu /> Detected Framework</div>
              <div className="pi-card-content" style={{ fontSize: '18px', fontWeight: 'bold', color: '#2563eb' }}>
                {data.framework}
              </div>
            </div>

            {/* Ingestion Support */}
            <div className="pi-card">
              <div className="pi-card-title"><FiDatabase /> Ingestion Support by Framework</div>
              <div className="pi-card-content">
                <div className="pi-tag-list">
                  <span className={`pi-tag ${data.ingestion_support?.file_based ? 'active' : 'inactive'}`}>File-based</span>
                  <span className={`pi-tag ${data.ingestion_support?.api ? 'active' : 'inactive'}`}>API</span>
                  <span className={`pi-tag ${data.ingestion_support?.database ? 'active' : 'inactive'}`}>Database/Table</span>
                  <span className={`pi-tag ${data.ingestion_support?.streaming ? 'active' : 'inactive'}`}>Streaming</span>
                  <span className={`pi-tag ${data.ingestion_support?.batch ? 'active' : 'inactive'}`}>Batch</span>
                </div>
              </div>
            </div>

            {/* File Types */}
            <div className="pi-card">
              <div className="pi-card-title"><FiFile /> File Types</div>
              <div className="pi-card-content">
                <div className="pi-tag-list">
                  {data.file_types?.map(ft => (
                    <span key={ft} className="pi-tag active">{ft}</span>
                  ))}
                  {(!data.file_types || data.file_types.length === 0) && (
                    <span className="pi-tag inactive">None Detected</span>
                  )}
                </div>
              </div>
            </div>

            {/* Delimiter Config */}
            <div className="pi-card">
              <div className="pi-card-title"><FiSettings /> Delimiter Config</div>
              <div className="pi-card-content">
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                  <div><strong>Delimiter:</strong> <span className="pi-tag">{data.delimiter_config?.column_delimiter}</span></div>
                  <div><strong>Quote:</strong> <span className="pi-tag">{data.delimiter_config?.quote_char}</span></div>
                  <div><strong>Escape:</strong> <span className="pi-tag">{data.delimiter_config?.escape_char}</span></div>
                  <div><strong>Header:</strong> <span className="pi-tag">{data.delimiter_config?.header ? 'true' : 'false'}</span></div>
                </div>
              </div>
            </div>

            {/* DQ Rules */}
            <div className="pi-card">
              <div className="pi-card-title"><FiActivity /> DQ Rules</div>
              <div className="pi-card-content">
                <ul style={{ margin: 0, paddingLeft: '20px', lineHeight: '1.6' }}>
                  {data.dq_rules?.schema_validation && <li>Schema validation</li>}
                  {data.dq_rules?.null_check && <li>Null checks</li>}
                  {data.dq_rules?.duplicate_check && <li>Duplicate checks</li>}
                  {data.dq_rules?.datatype_check && <li>Datatype validation</li>}
                </ul>
              </div>
            </div>
          </div>

          {/* Loading Flow */}
          <div className="pi-card" style={{ marginTop: '8px' }}>
            <div className="pi-card-title"><FiArrowRight /> Expected Workflow Structure</div>
            <div className="pi-card-content">
              <div className="pi-flow">
                {data.loading_flow?.map((step, idx) => (
                  <React.Fragment key={idx}>
                    <div className="pi-flow-step">{step}</div>
                    {idx < data.loading_flow.length - 1 && (
                      <div className="pi-flow-arrow"><FiArrowRight /></div>
                    )}
                  </React.Fragment>
                ))}
              </div>
            </div>
          </div>

          <div className="pi-actions">
            <button className="pi-btn-confirm" onClick={() => onConfirm(data)}>
              <FiCheck /> Review Data Sources
            </button>
          </div>
        </>
      )}
    </div>
  );
}
