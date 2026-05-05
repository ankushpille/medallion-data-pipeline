import { useState, useEffect } from 'react';
import { FiRefreshCw, FiSave, FiClock, FiZap } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import logo from '../../assets/images/image.png';

export default function StepConfig({
  selectedClient, folderPath, sourceType, call, toast, onNext, syncMasterConfig, intelligenceData, fabricMode = 'DISCOVERY', setConfigPersisted
}) {
  const navigate = useNavigate();
  const [configData, setConfigData] = useState([]);
  const [generatedConfigText, setGeneratedConfigText] = useState('');
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (selectedClient) loadConfig();
  }, [selectedClient, folderPath]);

  useEffect(() => {
    if (intelligenceData?.reformatted_config) {
      setGeneratedConfigText(JSON.stringify(intelligenceData.reformatted_config, null, 2));
    }
  }, [intelligenceData]);

  async function loadConfig() {
    setLoading(true);
    try {
      await syncMasterConfig();
      const res = await call(`/orchestrate/master-config?client_name=${encodeURIComponent(selectedClient)}&source_type=${encodeURIComponent(sourceType || '')}&dataset_ids=${encodeURIComponent(folderPath || '')}`);
      console.debug('Master config fetch response', {
        client_name: selectedClient,
        sourceType,
        folderPath,
        rows: res?.config?.length || 0,
        message: res?.message,
      });
      
      // Auto-fill logic from Intelligence Scan
      let loadedConfig = res.config || [];
      if (intelligenceData && loadedConfig.length > 0) {
        loadedConfig = loadedConfig.map(row => {
          const newRow = { ...row };
          if (intelligenceData.delimiter_config) {
            if ('delimiter' in newRow || 'column_delimiter' in newRow) {
               const key = 'delimiter' in newRow ? 'delimiter' : 'column_delimiter';
               newRow[key] = intelligenceData.delimiter_config.column_delimiter;
            }
            if ('quote_char' in newRow) newRow.quote_char = intelligenceData.delimiter_config.quote_char;
            if ('escape_char' in newRow) newRow.escape_char = intelligenceData.delimiter_config.escape_char;
            if ('header' in newRow) newRow.header = intelligenceData.delimiter_config.header ? 'true' : 'false';
          }
          return newRow;
        });
      }
      setConfigData(loadedConfig);
    } catch (e) {
      toast('Failed to load master configuration', 'error');
    } finally {
      setLoading(false);
    }
  }

  async function saveConfig() {
    setSaving(true);
    
    // Prepare payload
    let payload = {
      client_name: selectedClient || "fabric_client",
      config: configData
    };

    if (fabricMode === "DEPLOY" && configData.length === 0 && intelligenceData?.reformatted_config) {
        // Construct a single config row from intelligence data if table is empty
        const extractedJson = intelligenceData.reformatted_config;
        const autoRow = {
            dataset_id: extractedJson.dataset_id || `fabric_${Date.now()}`,
            client_name: selectedClient || "fabric_client",
            source_type: "FABRIC",
            source_folder: extractedJson.source_path || "fabric://pipeline",
            source_object: extractedJson.pipeline_name || extractedJson.name || "FabricPipeline",
            file_format: (extractedJson.file_types && extractedJson.file_types[0]) || "JSON",
            is_active: true,
            load_type: "full",
        };
        payload.config = [autoRow];
    }

    // Fix 2: Prevent EMPTY VALUES
    if (!payload.client_name || !payload.config || payload.config.length === 0) {
        console.error("Invalid payload:", payload);
        toast('Configuration data is empty. Please sync or edit before saving.', 'warning');
        setSaving(false);
        return;
    }

    try {
      const res = await call('/orchestrate/master-config/update', 'POST', payload);
      if (res && res.status === 'SUCCESS') {
          toast('Configuration saved successfully', 'success');
          setConfigPersisted?.(true);
      } else {
          throw new Error('Save failed');
      }
    } catch (e) {
      toast('Failed to save configuration', 'error');
      setConfigPersisted?.(false);
    } finally {
      setSaving(false);
    }
  }

  const columns = configData.length > 0 ? Object.keys(configData[0]) : [];

  return (
    <motion.div
      key="step3"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 30 }}>
        <div>
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900 }}>Master Configuration — {selectedClient}</h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8 }}>Review and edit ingestion parameters.</p>
        </div>
        <img src={logo} alt="Agilisium" style={{ height: 32, objectFit: 'contain' }} />
      </div>

      <div className="step-body config-table-container">
        {/* Utility Bar */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 20, gap: 10 }}>
          <button className="orch-btn ghost tiny" onClick={loadConfig} disabled={loading} style={{ height: 32, display: 'flex', alignItems: 'center' }}>
            <FiRefreshCw className={loading ? 'spin' : ''} style={{ marginRight: 6 }} /> Reload
          </button>
          <button 
            className="orch-btn ghost tiny" 
            onClick={async () => {
              setLoading(true);
              try {
                await syncMasterConfig();
                await loadConfig();
                toast('Master configuration synced and reloaded successfully', 'success');
              } catch (e) {
                toast('Sync failed', 'error');
              } finally {
                setLoading(false);
              }
            }} 
            disabled={loading} 
            style={{ height: 32, display: 'flex', alignItems: 'center' }}
          >
            <FiZap className={loading ? 'spin' : ''} style={{ marginRight: 6 }} /> Sync Config
          </button>
        </div>
        
        {intelligenceData && (
          <div style={{ marginBottom: 20, padding: 12, background: 'rgba(59, 130, 246, 0.1)', border: '1px solid rgba(59, 130, 246, 0.2)', borderRadius: 8, display: 'flex', alignItems: 'center', gap: 12, color: '#1d4ed8' }}>
            <FiZap size={20} />
            <div style={{ fontSize: 13, fontWeight: 500 }}>
              <strong>Auto-generated Configuration:</strong> The settings below have been pre-filled using the intelligent scan results (Framework: {intelligenceData.framework}, Delimiters, Formats).
            </div>
          </div>
        )}

        {loading ? (
          <div className="config-table-wrapper">
            <table className="config-table">
              <thead>
                <tr>
                  <th className="row-num"><div className="skeleton" style={{ height: 14, width: 20 }} /></th>
                  {Array.from({ length: 6 }).map((_, i) => (
                    <th key={i}><div className="skeleton" style={{ height: 16, width: '60%' }} /></th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {Array.from({ length: 8 }).map((_, ridx) => (
                  <tr key={ridx}>
                    <td className="row-num"><div className="skeleton" style={{ height: 14, width: 20 }} /></td>
                    {Array.from({ length: 6 }).map((_, cidx) => (
                      <td key={cidx}><div className="skeleton" style={{ height: 28, borderRadius: 6 }} /></td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : configData.length === 0 && intelligenceData ? (
          <div className="empty-source" style={{ padding: 24, textAlign: 'left', display: 'flex', flexDirection: 'column', gap: 14 }}>
            <div>
              <strong>Generated config from scan</strong>
              <div className="step-sub" style={{ marginTop: 4 }}>Review and edit this JSON before sending the execution payload forward.</div>
            </div>
            <textarea
              className="orch-input"
              rows={16}
              value={generatedConfigText}
              onChange={(e) => setGeneratedConfigText(e.target.value)}
              style={{ fontFamily: 'monospace', fontSize: 12, lineHeight: 1.5, width: '100%', resize: 'vertical' }}
            />
          </div>
        ) : configData.length === 0 ? (
          <div className="empty-source" style={{ padding: 60, textAlign: 'center', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
            <p style={{ marginBottom: 20 }}>No configuration found for these sources in the master config registry.</p>
            <button 
              className="orch-btn primary" 
              onClick={async () => {
                setLoading(true);
                try {
                  await call('/orchestrate/initialize', 'POST', {
                    source_type: sourceType,
                    client_name: selectedClient,
                    folder_path: folderPath
                  });
                  toast('Master registry initialized successfully.', 'success');
                  loadConfig();
                } catch (e) {
                  toast('Sync failed: Master Config CSV does not exist yet for this source.', 'warning');
                } finally {
                  setLoading(false);
                }
              }}
              disabled={loading}
            >
              <FiRefreshCw className={loading ? 'spin' : ''} style={{ marginRight: 8 }} />
              Discover & Sync Now
            </button>
          </div>
        ) : (
          <div className="config-table-wrapper">
            <table className="config-table">
              <thead>
                <tr>
                  <th className="row-num">#</th>
                  {columns.map(k => (
                    <th key={k}>{k.replace(/_/g, ' ')}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {configData.map((row, ridx) => (
                  <tr key={ridx} className={ridx % 2 === 0 ? 'even' : 'odd'}>
                    <td className="row-num">{ridx + 1}</td>
                    {columns.map((k, vidx) => (
                      <td key={vidx}>
                        <input
                          className="config-cell-input"
                          value={row[k] || ''}
                          onChange={(e) => {
                            const newData = [...configData];
                            newData[ridx] = { ...newData[ridx], [k]: e.target.value };
                            setConfigData(newData);
                          }}
                        />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="step-footer" style={{ justifyContent: 'space-between' }}>
        <div className="step-footer-info">
          {configData.length > 0 && <span>{configData.length} rows · {columns.length} columns</span>}
        </div>
        <div style={{ display: 'flex', gap: 10 }}>
          <button
            className="orch-btn ghost"
            onClick={saveConfig}
            disabled={saving || loading || !(configData.length > 0 || (fabricMode === 'DEPLOY' && !!intelligenceData?.reformatted_config))}
          >
            <FiSave style={{ marginRight: 6 }} /> {saving ? 'Saving...' : 'Save Config'}
          </button>
          <button
            className="orch-btn primary step-next-btn"
            onClick={async () => {
              if (configData.length > 0) await saveConfig();
              onNext();
            }}
            disabled={loading || (configData.length === 0 && !generatedConfigText)}
          >
            Commit & Continue →
          </button>
        </div>
      </div>
    </motion.div>
  );
}
