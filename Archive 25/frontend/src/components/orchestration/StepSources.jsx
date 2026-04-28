import { FiCheck, FiZap, FiFolder, FiFile, FiChevronRight, FiLink, FiBox, FiCloud } from 'react-icons/fi';
import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import FluentSelect from '../FluentSelect';
import logo from '../../assets/images/image.png';
import '../PipelineIntelligence.css';

export default function StepSources({
  selectedClient, apiSources, s3Sources = [], adlsSources = [], apiSourcesLoading,
  selectedApiSource, setSelectedApiSource,
  selectedEndpoint, setSelectedEndpoint,
  sourceType, setSourceType, setFolderPath,
  setShowUploadModal, openExplorer, onNext, call,
  refreshTrigger, intelligenceData, configPersisted, setConfigPersisted, toast
}) {
  const [localFiles, setLocalFiles] = useState([]);
  const [localFilesLoading, setLocalFilesLoading] = useState(false);
  const [selectedLocalFiles, setSelectedLocalFiles] = useState([]);
  const [savingGeneratedConfig, setSavingGeneratedConfig] = useState(false);
  const [saveGeneratedError, setSaveGeneratedError] = useState('');

  useEffect(() => {
    if (selectedClient) {
      setSelectedLocalFiles([]); // Clear selections when switching clients
      setLocalFiles([]); // Reset list to show loading state if needed
      fetchLocalFiles(selectedClient);
    }
  }, [selectedClient, refreshTrigger]);

  async function fetchLocalFiles(client) {
    setLocalFilesLoading(true);
    try {
      const data = await call(`/upload/list?client_name=${client}`);
      setLocalFiles(data.files || []);
    } catch (e) {
      setLocalFiles([]);
    } finally {
      setLocalFilesLoading(false);
    }
  }

  const toggleLocalFile = (file) => {
    let newSelected;
    if (selectedLocalFiles.includes(file.dataset_id)) {
      newSelected = selectedLocalFiles.filter(id => id !== file.dataset_id);
    } else {
      newSelected = [...selectedLocalFiles, file.dataset_id];
    }
    setSelectedLocalFiles(newSelected);
    
    if (newSelected.length > 0) {
      setSelectedApiSource(`local-multi`);
      const idList = newSelected.join(',');
      setSelectedEndpoint(idList);
      setSourceType('LOCAL');
      setFolderPath(idList); // Pass IDs in folder_path for LOCAL
    } else {
      setSelectedApiSource(null);
      setSelectedEndpoint('');
      setSourceType('LOCAL');
      setFolderPath('');
    }
  }

  const [activeTab, setActiveTab] = useState('LOCAL');

  useEffect(() => {
    if (intelligenceData?.ingestion_support) {
      const details = intelligenceData.ingestion_details || intelligenceData.reformatted_config || {};
      const detectedSourceType = details.source_type || intelligenceData.reformatted_config?.source_type;
      const detectedSourcePath = details.source_path || intelligenceData.reformatted_config?.source_path;
      if (detectedSourceType) setSourceType(detectedSourceType);
      if (detectedSourcePath) {
        setFolderPath(detectedSourcePath);
        setSelectedEndpoint(detectedSourcePath);
        setSelectedApiSource('intelligence-scan');
      }

      if (detectedSourceType === 'S3') {
        setActiveTab('S3');
      } else if (detectedSourceType === 'ADLS') {
        setActiveTab('ADLS');
      } else if (detectedSourceType === 'LOCAL') {
        setActiveTab('LOCAL');
      } else if (intelligenceData.ingestion_support.api) {
        setActiveTab('API');
      } else if (intelligenceData.ingestion_support.file_based) {
        setActiveTab('S3'); // Default to cloud storage
      }
    }
  }, [intelligenceData, selectedEndpoint, setFolderPath, setSelectedApiSource, setSelectedEndpoint, setSourceType]);

  const isTabSupported = (tabId) => {
    if (!intelligenceData?.ingestion_support) return true;
    if (tabId === 'LOCAL') return true;
    if (tabId === 'API') return !!intelligenceData.ingestion_support.api;
    if (tabId === 'S3') return !!intelligenceData.ingestion_support.file_based && intelligenceData.ingestion_details?.target !== 'azure';
    if (tabId === 'ADLS') return !!intelligenceData.ingestion_support.file_based && intelligenceData.ingestion_details?.target !== 'aws';
    return true;
  };

  const sourceTabs = [
    { id: 'LOCAL', label: 'Local Files', icon: <FiFolder />, color: '#10b981' },
    { id: 'API', label: 'REST API', icon: <FiLink />, color: '#3b82f6' },
    { id: 'S3', label: 'AWS S3', icon: <FiBox />, color: '#f59e0b' },
    { id: 'ADLS', label: 'Azure ADLS', icon: <FiCloud />, color: '#0078d4' }
  ];

  const scanDetails = intelligenceData?.ingestion_details || intelligenceData?.reformatted_config || {};
  const detectedSourceType = sourceType || scanDetails.source_type || intelligenceData?.reformatted_config?.source_type || sourceTabs.find(t => t.id === activeTab)?.id;
  const detectedSourcePath = selectedEndpoint || scanDetails.source_path || intelligenceData?.reformatted_config?.source_path || '';
  const support = intelligenceData?.ingestion_support || {};
  const ingestionRows = [
    { key: 'file_based', label: 'File-based ingestion' },
    { key: 'api', label: 'API ingestion' },
    { key: 'database', label: 'Database ingestion' },
    { key: 'streaming', label: 'Streaming' },
    { key: 'batch', label: 'Batch' },
  ];

  async function saveIntelligenceConfigAndContinue() {
    if (!intelligenceData) {
      onNext();
      return;
    }
    if (intelligenceData.is_fallback || intelligenceData.scan_status === 'failed' || intelligenceData.auth_mode === 'none' || intelligenceData.pipeline_capabilities?.scan_mode === 'mock') {
      const msg = 'Please perform a real scan using credentials before execution.';
      setSaveGeneratedError(msg);
      toast?.(msg, 'error');
      return;
    }

    setSavingGeneratedConfig(true);
    setSaveGeneratedError('');
    try {
      console.debug('Saving intelligenceData before Step 4', {
        framework: intelligenceData.framework,
        scan_status: intelligenceData.scan_status,
        auth_mode: intelligenceData.auth_mode,
        is_fallback: intelligenceData.is_fallback,
        source_path: detectedSourcePath,
      });
      const response = await call('/config/save', 'POST', {
        client_name: selectedClient,
        intelligence_data: intelligenceData,
        source_type: detectedSourceType,
        source_path: detectedSourcePath,
      });
      console.debug('Config save API response', response);
      setConfigPersisted?.(true);
      toast?.(`Saved ${response.rows_inserted || 0} configuration row(s)`, 'success');
      onNext();
    } catch (e) {
      const msg = e?.message || 'Failed to save generated configuration';
      setSaveGeneratedError(msg);
      toast?.(msg, 'error');
    } finally {
      setSavingGeneratedConfig(false);
    }
  }

  const renderScanDrivenSources = () => (
    <div className="step-body">
      <div style={{ marginBottom: 20, padding: 12, background: 'rgba(16, 185, 129, 0.1)', border: '1px solid rgba(16, 185, 129, 0.2)', borderRadius: 8, display: 'flex', alignItems: 'center', gap: 12, color: '#047857' }}>
        <FiZap size={20} />
        <div style={{ fontSize: 13, fontWeight: 600 }}>
          <strong>Auto-detected from Pipeline Intelligence.</strong> Confirm the detected source and ingestion modes before configuration.
        </div>
      </div>

      <div className="pi-grid">
        {ingestionRows.map(row => {
          const supported = !!support[row.key];
          return (
            <div key={row.key} className="pi-card" style={{ opacity: supported ? 1 : 0.55 }}>
              <div className="pi-card-title">{row.label}</div>
              <div className="pi-card-content">
                <span className={`pi-tag ${supported ? 'active' : 'inactive'}`}>
                  {supported ? 'Supported' : 'Not Detected'}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      <div className="pi-card" style={{ marginTop: 16 }}>
        <div className="pi-card-title">Detected Source</div>
        <div style={{ display: 'grid', gridTemplateColumns: '180px 1fr', gap: 12, alignItems: 'center' }}>
          <label className="cloud-scan-field">
            <span>Source Type</span>
            <select
              className="orch-input"
              value={detectedSourceType || ''}
              onChange={(e) => setSourceType(e.target.value)}
            >
              <option value="S3">S3</option>
              <option value="ADLS">ADLS</option>
              <option value="API">API</option>
              <option value="LOCAL">LOCAL</option>
            </select>
          </label>
          <label className="cloud-scan-field">
            <span>Suggested source path / bucket / API / table</span>
            <input
              className="orch-input"
              value={detectedSourcePath}
              onChange={(e) => {
                setFolderPath(e.target.value);
                setSelectedEndpoint(e.target.value);
                setSelectedApiSource('intelligence-scan');
              }}
            />
          </label>
        </div>
      </div>

      <div className="pi-card" style={{ marginTop: 16 }}>
        <div className="pi-card-title">Formats</div>
        <div className="pi-tag-list">
          {(intelligenceData?.file_types || []).map(ft => <span key={ft} className="pi-tag active">{ft}</span>)}
          {(!intelligenceData?.file_types || intelligenceData.file_types.length === 0) && <span className="pi-tag inactive">None Detected</span>}
        </div>
      </div>

      <div className="pi-card" style={{ marginTop: 16 }}>
        <div className="pi-card-title">Detected Ingestion Types</div>
        <div className="pi-tag-list">
          {(intelligenceData?.ingestion_types || []).map(mode => (
            <span key={mode} className="pi-tag active">{mode.replace(/_/g, ' ')}</span>
          ))}
          {(!intelligenceData?.ingestion_types || intelligenceData.ingestion_types.length === 0) && <span className="pi-tag inactive">None Detected</span>}
        </div>
      </div>

      <div className="step-footer">
        {saveGeneratedError && <div className="panel-error-alert" style={{ marginRight: 'auto' }}>{saveGeneratedError}</div>}
        {configPersisted && <div className="config-chip" style={{ marginRight: 'auto' }}><strong>Config:</strong> Saved</div>}
        <button
          className="orch-btn primary step-next-btn"
          disabled={!detectedSourcePath || savingGeneratedConfig || intelligenceData?.is_fallback || intelligenceData?.scan_status === 'failed' || intelligenceData?.auth_mode === 'none' || intelligenceData?.pipeline_capabilities?.scan_mode === 'mock'}
          onClick={saveIntelligenceConfigAndContinue}
        >
          {savingGeneratedConfig ? 'Saving Config...' : 'Continue to Configuration →'}
        </button>
      </div>
    </div>
  );

  return (
    <motion.div
      key="step2"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24, paddingBottom: 24, borderBottom: '1px solid rgba(0,0,0,0.05)' }}>
        <div style={{ flex: 1 }}>
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900, background: 'linear-gradient(90deg, var(--text1), var(--text2))', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>Data Sources — {selectedClient}</h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8, fontSize: 13, fontWeight: 500 }}>Choose an existing source or endpoint to begin.</p>
        </div>
        
        <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
          {!intelligenceData && <div className="source-tabs" style={{ display: 'flex', gap: 4, background: 'var(--surface2)', padding: 4, borderRadius: 14 }}>
            {sourceTabs.map(t => (
              <button
                key={t.id}
                className={`source-tab-btn ${activeTab === t.id ? 'active' : ''}`}
                disabled={!isTabSupported(t.id)}
                onClick={() => isTabSupported(t.id) && setActiveTab(t.id)}
                style={{
                  padding: '8px 16px', borderRadius: 11, border: 'none',
                  background: 'transparent',
                  color: activeTab === t.id ? t.color : 'var(--text3)',
                  fontWeight: 700, cursor: isTabSupported(t.id) ? 'pointer' : 'not-allowed', display: 'flex', alignItems: 'center', gap: 8,
                  fontSize: 12, transition: 'all 0.3s', position: 'relative',
                  opacity: isTabSupported(t.id) ? 1 : 0.45
                }}
              >
                {activeTab === t.id && (
                  <motion.div
                    layoutId="active-source-pill"
                    className="active-tab-indicator"
                    style={{
                      position: 'absolute', inset: 0,
                      background: '#fff', borderRadius: 11,
                      boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
                      zIndex: 2
                    }}
                    transition={{ type: "spring", bounce: 0.2, duration: 0.4 }}
                  />
                )}
                <div style={{ position: 'relative', zIndex: 3, display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ fontSize: 16 }}>{t.icon}</span>
                  {t.label}
                  {t.id === 'LOCAL' && localFiles.length > 0 && <span className="tab-badge" style={{ background: t.color }}>{localFiles.length}</span>}
                  {t.id === 'API' && apiSources.length > 0 && <span className="tab-badge" style={{ background: t.color }}>{apiSources.length}</span>}
                </div>
              </button>
            ))}
          </div>}
          
          <div className="header-logo-divider" style={{ width: 1, height: 24, background: 'rgba(0,0,0,0.1)', marginLeft: 8 }} />
          <img src={logo} alt="Agilisium" style={{ height: 28, objectFit: 'contain' }} />
        </div>
      </div>

      {intelligenceData ? renderScanDrivenSources() : (

      <div className="step-body">
        <div className="tab-content" style={{ minHeight: 300 }}>
          {/* API Sources Tab */}
          {activeTab === 'API' && (
            <div className="source-section animate-in">
              <div className="source-list">
                {apiSourcesLoading ? (
                  [1, 2].map(i => <div key={i} className="skeleton" style={{ height: 72, borderRadius: 14, marginBottom: 12 }} />)
                ) : apiSources.length > 0 ? (
                  apiSources.map(s => (
                    <div
                      key={s.id}
                      className={`source-card ${selectedApiSource === s.id ? 'selected' : ''}`}
                      onClick={() => {
                        setSelectedApiSource(s.id);
                        if (s.endpoints && s.endpoints.length > 0 && !selectedEndpoint) {
                           setSelectedEndpoint(s.endpoints[0]);
                           setSourceType('API');
                           setFolderPath(s.endpoints[0]);
                        }
                      }}
                    >
                      <div className="source-info" style={{ flex: 1 }}>
                        <div className="source-name">{s.source_name}</div>
                        <div className="source-url">{s.base_url}</div>
                      </div>
                      <div className="source-actions" onClick={(e) => e.stopPropagation()} style={{ minWidth: 220 }}>
                        <FluentSelect
                          multi
                          style={{ minWidth: 220 }}
                          value={selectedApiSource === s.id ? (selectedEndpoint ? selectedEndpoint.split(',') : []) : []}
                          placeholder="Select endpoints..."
                          onChange={(e) => {
                            const vals = e.target.value;
                            setSelectedApiSource(s.id);
                            setSelectedEndpoint(vals.join(','));
                            setSourceType('API');
                            setFolderPath(vals.join(','));
                          }}
                          options={(s.endpoints || []).map(ep => ({ value: ep, label: ep }))}
                        />
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="empty-source">No API sources registered for {selectedClient}.</div>
                )}
              </div>
            </div>
          )}

          {/* Local Tab */}
          {activeTab === 'LOCAL' && (
            <div className="source-section animate-in">
              <div className="local-layout">
                 <div className="source-card upload-trigger" onClick={() => setShowUploadModal(true)}>
                  <div className="source-info">
                    <div className="source-name">Upload New File</div>
                    <div className="source-url">Target: Raw/{selectedClient}/...</div>
                  </div>
                  <button className="orch-btn primary tiny">Upload Now</button>
                </div>

                <div className="local-list" style={{ marginTop: 20 }}>
                  <div className="sub-title" style={{ fontSize: 13, color: 'var(--text2)', marginBottom: 12, fontWeight: 700 }}>Previously Uploaded Datasets</div>
                  {localFilesLoading ? (
                    [1, 2, 3].map(i => <div key={i} className="skeleton" style={{ height: 50, borderRadius: 10, marginBottom: 8 }} />)
                  ) : localFiles.length === 0 ? (
                    <div className="empty-local">No previous uploads found.</div>
                  ) : (
                    localFiles.map(file => (
                      <div
                        key={file.dataset_id}
                        className={`local-file-card ${selectedLocalFiles.includes(file.dataset_id) ? 'selected' : ''}`}
                        onClick={() => toggleLocalFile(file)}
                      >
                        <div className="file-icon"><FiFile size={16} /></div>
                        <div className="file-info">
                          <div className="file-name">{file.source_object}</div>
                          <div className="file-meta">
                             <span>{file.file_format}</span> • <span>{new Date(file.created_at).toLocaleDateString()}</span>
                          </div>
                        </div>
                        {selectedLocalFiles.includes(file.dataset_id) && <div className="file-check"><FiCheck size={14} /></div>}
                      </div>
                    ))
                  )}
                </div>
              </div>
            </div>
          )}

          {/* AWS S3 Tab (Placeholder) */}
          {/* AWS S3 Tab */}
          {activeTab === 'S3' && (
            <div className="source-section animate-in">
              <div className="source-list">
                {s3Sources.length > 0 ? (
                  s3Sources.map(s => (
                    <div
                      key={s.id}
                      className={`source-card ${selectedApiSource === s.id ? 'selected' : ''}`}
                      onClick={() => {
                        setSelectedApiSource(s.id);
                        setSourceType('S3');
                      }}
                    >
                      <div className="source-info" style={{ flex: 1 }}>
                        <div className="source-name">{s.source_name} (AWS S3)</div>
                        <div className="source-url">{s.bucket_name}</div>
                        {selectedApiSource === s.id && selectedEndpoint && (
                           <div className="selected-path-msg" style={{ fontSize: 11, color: 'var(--blue)', marginTop: 4, fontWeight: 700 }}>
                              📁 Selected: {selectedEndpoint}
                           </div>
                        )}
                      </div>
                      <div className="source-actions">
                        <button 
                          className="orch-btn tiny"
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedApiSource(s.id);
                            setSourceType('S3');
                            const basePath = `s3://${s.bucket_name}`;
                            openExplorer(basePath, 'pick', (path) => {
                              setSelectedEndpoint(path);
                              setFolderPath(path);
                            });
                          }}
                        >
                          {selectedEndpoint && selectedApiSource === s.id ? 'Change Folder' : 'Browse Storage'}
                        </button>
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="empty-state" style={{ textAlign: 'center', padding: '40px 0' }}>
                    <FiFolder size={48} style={{ opacity: 0.2, marginBottom: 16 }} />
                    <div style={{ fontWeight: 600, color: 'var(--text2)' }}>No S3 Buckets Connected</div>
                    <div style={{ fontSize: 12, color: 'var(--text3)', marginTop: 4 }}>Register a new S3 source in Step 1 to see it here.</div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Azure ADLS Tab */}
          {activeTab === 'ADLS' && (
            <div className="source-section animate-in">
              <div className="source-list">
                {adlsSources.length > 0 ? (
                  adlsSources.map(s => (
                    <div
                      key={s.id}
                      className={`source-card ${selectedApiSource === s.id ? 'selected' : ''}`}
                      onClick={() => {
                        setSelectedApiSource(s.id);
                        setSourceType('ADLS');
                      }}
                    >
                      <div className="source-info" style={{ flex: 1 }}>
                        <div className="source-name">{s.source_name} (Azure ADLS)</div>
                        <div className="source-url">{s.azure_account}/{s.azure_container}</div>
                        {selectedApiSource === s.id && selectedEndpoint && (
                           <div className="selected-path-msg" style={{ fontSize: 11, color: 'var(--blue)', marginTop: 4, fontWeight: 700 }}>
                              📁 Selected: {selectedEndpoint}
                           </div>
                        )}
                      </div>
                      <div className="source-actions">
                        <button 
                          className="orch-btn tiny"
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedApiSource(s.id);
                            setSourceType('ADLS');
                            const basePath = `az://${s.azure_account}/${s.azure_container}`;
                            openExplorer(basePath, 'pick', (path) => {
                              setSelectedEndpoint(path);
                              setFolderPath(path);
                            });
                          }}
                        >
                          {selectedEndpoint && selectedApiSource === s.id ? 'Change Folder' : 'Browse Storage'}
                        </button>
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="empty-state" style={{ textAlign: 'center', padding: '40px 0' }}>
                    <FiChevronRight size={48} style={{ opacity: 0.2, marginBottom: 16 }} />
                    <div style={{ fontWeight: 600, color: 'var(--text2)' }}>No ADLS Containers Connected</div>
                    <div style={{ fontSize: 12, color: 'var(--text3)', marginTop: 4 }}>Register a new ADLS source in Step 1 to see it here.</div>
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Continue */}
        <div className="step-footer">
          <button
            className="orch-btn primary step-next-btn"
            disabled={!selectedEndpoint && !intelligenceData}
            onClick={() => onNext()}
          >
            Continue to Configuration →
          </button>
        </div>
      </div>
      )}
    </motion.div>
  );
}


