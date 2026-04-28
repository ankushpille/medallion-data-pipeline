import { FiCheck, FiZap, FiFolder, FiFile, FiChevronRight, FiRefreshCw, FiClock, FiLink, FiBox, FiCloud, FiSearch } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';
import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import FluentSelect from '../FluentSelect';
import logo from '../../assets/images/image.png';
import CloudPortalScanModal from './CloudPortalScanModal';

export default function StepSources({
  selectedClient, apiSources, s3Sources = [], adlsSources = [], apiSourcesLoading,
  selectedApiSource, setSelectedApiSource,
  selectedEndpoint, setSelectedEndpoint,
  setSourceType, setFolderPath,
  setShowUploadModal, openExplorer, onNext, call,
  refreshTrigger, intelligenceData, setIntelligenceData
}) {
  const navigate = useNavigate();
  const [localFiles, setLocalFiles] = useState([]);
  const [localFilesLoading, setLocalFilesLoading] = useState(false);
  const [selectedLocalFiles, setSelectedLocalFiles] = useState([]);
  const [showCloudScanModal, setShowCloudScanModal] = useState(false);

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
      const details = intelligenceData.ingestion_details || {};
      if (details.source_type && details.source_path && !selectedEndpoint) {
        setSourceType(details.source_type);
        setFolderPath(details.source_path);
        setSelectedEndpoint(details.source_path);
        setSelectedApiSource('intelligence-scan');
      }

      if (details.source_type === 'S3') {
        setActiveTab('S3');
      } else if (details.source_type === 'ADLS') {
        setActiveTab('ADLS');
      } else if (details.source_type === 'FABRIC') {
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
          <div className="source-tabs" style={{ display: 'flex', gap: 4, background: 'var(--surface2)', padding: 4, borderRadius: 14 }}>
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
          </div>
          <button
            className="orch-btn tiny"
            onClick={() => setShowCloudScanModal(true)}
            style={{ display: 'flex', alignItems: 'center', gap: 6, whiteSpace: 'nowrap' }}
          >
            <FiSearch /> Scan Framework
          </button>
          
          <div className="header-logo-divider" style={{ width: 1, height: 24, background: 'rgba(0,0,0,0.1)', marginLeft: 8 }} />
          <img src={logo} alt="Agilisium" style={{ height: 28, objectFit: 'contain' }} />
        </div>
      </div>

      {intelligenceData && (
        <div style={{ marginBottom: 20, padding: 12, background: 'rgba(16, 185, 129, 0.1)', border: '1px solid rgba(16, 185, 129, 0.2)', borderRadius: 8, display: 'flex', alignItems: 'center', gap: 12, color: '#047857' }}>
          <FiZap size={20} />
          <div style={{ fontSize: 13, fontWeight: 500 }}>
            <strong>Auto-detected capabilities:</strong> The intelligent scan detected 
            {intelligenceData.ingestion_support?.api ? ' API' : ''}
            {intelligenceData.ingestion_support?.file_based ? ' File-based' : ''}
            {intelligenceData.ingestion_support?.database ? ' Database' : ''} 
            ingestion. Supported formats: {intelligenceData.file_types?.join(', ') || 'Unknown'}.
            {intelligenceData.ingestion_details?.source_path && (
              <span> Suggested source: {intelligenceData.ingestion_details.source_path}.</span>
            )}
          </div>
        </div>
      )}

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
                          className="orch-btn tiny ghost"
                          onClick={(e) => {
                            e.stopPropagation();
                            setShowCloudScanModal(true);
                          }}
                        >
                          <FiSearch style={{ marginRight: 6 }} /> Scan Framework
                        </button>
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
                    <button className="orch-btn tiny" onClick={() => setShowCloudScanModal(true)} style={{ marginTop: 14 }}>
                      <FiSearch style={{ marginRight: 6 }} /> Scan Framework
                    </button>
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
      {showCloudScanModal && (
        <CloudPortalScanModal
          selectedClient={selectedClient}
          onClose={() => setShowCloudScanModal(false)}
          onScanComplete={(result) => {
            setIntelligenceData?.(result);
            const details = result?.ingestion_details || result?.reformatted_config || {};
            const nextSourceType = details.source_type;
            const nextPath = details.source_path;
            if (nextSourceType) setSourceType(nextSourceType);
            if (nextPath) {
              setFolderPath(nextPath);
              setSelectedEndpoint(nextPath);
              setSelectedApiSource('framework-scan');
            }
          }}
        />
      )}
    </motion.div>
  );
}


