import { FiBarChart2, FiEdit2, FiZap, FiRefreshCw, FiX, FiClipboard, FiClock } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';
import { motion } from 'framer-motion';
import FluentSelect from '../FluentSelect';
import logo from '../../assets/images/image.png';

const itemVariants = {
  initial: { opacity: 0, y: 10 },
  animate: { opacity: 1, y: 0 }
};

export default function StepDQ({
  selectedClient, sourceType, folderPath,
  datasets, fetchDatasets,
  editingConfigDataset, setEditingConfigDataset,
  editingConfigColumns, editingConfigLoading,
  selectedDqDataset, setSelectedDqDataset,
  showDQPanel, setShowDQPanel,
  loadDqConfig, setPendingDqDataset, setShowModeModal,
  dqError, setDqError, dqLoading, isSuggesting,
  editingRuleDrafts, setEditingRuleDrafts,
  toggleColumnActive, changeColumnSeverity, saveColumnRule,
  saveDqConfig, editingConfigSaving,
  formatDatasetLabel, onNext, onRunOrchestration, isOrchestrating,
  datasetsLoading
}) {
  const navigate = useNavigate();
  return (
    <motion.div
      key="step4"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 30 }}>
        <div>
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900 }}>DQ Configuration — {selectedClient}</h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8 }}>Review datasets and configure Data Quality rules.</p>
        </div>
        <img src={logo} alt="Agilisium" style={{ height: 32, objectFit: 'contain' }} />
      </div>

      <div className="step-body">
        {/* Utility Bar */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 20 }}>
          <button className="orch-btn ghost tiny" onClick={fetchDatasets} style={{ height: 32, display: 'flex', alignItems: 'center' }}>
            <FiRefreshCw style={{ marginRight: 6 }} /> Refresh
          </button>
        </div>
        {/* Config Summary */}
        <div className="dq-config-summary">
          <div className="config-chip"><strong>Client:</strong> {selectedClient}</div>
          <div className="config-chip"><strong>Source:</strong> {sourceType}</div>
          <div className="config-chip"><strong>Endpoint:</strong> {folderPath}</div>
          <div className="config-chip"><strong>Default DQ:</strong> null_check, schema_validation, datatype_check as warnings</div>
        </div>

        {/* Detected Datasets */}
        <div className="source-section">
          <div className="source-section-title">
            <FiBarChart2 style={{ marginRight: 8 }} />
            Detected Datasets ({datasets.length})
          </div>
          <div className="source-list">
            {datasetsLoading ? (
              <div className="source-list" style={{ width: '100%' }}>
                {[1, 2, 3].map(i => (
                  <div key={i} className="source-card" style={{ gap: 16, cursor: 'default' }}>
                    <div className="skeleton-circle" style={{ width: 44, height: 44, borderRadius: 12, flexShrink: 0 }} />
                    <div className="source-info" style={{ flex: 1 }}>
                      <div className="skeleton" style={{ height: 16, width: '40%', marginBottom: 8 }} />
                      <div className="skeleton" style={{ height: 12, width: '70%' }} />
                    </div>
                    <div className="skeleton" style={{ height: 32, width: 80, borderRadius: 8 }} />
                  </div>
                ))}
              </div>
            ) : datasets.length === 0 ? (
              <div className="empty-source">No datasets found yet. Continue is allowed; DQ warnings will be prepared during execution.</div>
            ) : (
              datasets.map((d, i) => (
                <motion.div
                  key={d.dataset_id}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0, transition: { delay: i * 0.05 } }}
                  className={`source-card ${editingConfigDataset === d.dataset_id ? 'selected' : ''}`}
                >
                  <div className="source-info">
                    <div className="source-name">{d.dataset_name || d.dataset_id}</div>
                    <div className="source-url">{d.dataset_id}</div>
                  </div>
                  <div className="source-actions">
                    <button
                      className="orch-btn tiny"
                      onClick={() => loadDqConfig(d.dataset_id)}
                      disabled={editingConfigLoading && editingConfigDataset === d.dataset_id}
                      style={{ display: 'flex', alignItems: 'center', gap: 6, fontWeight: 800 }}
                    >
                      {editingConfigLoading && editingConfigDataset === d.dataset_id ? (
                        <FiRefreshCw className="spin" />
                      ) : (
                        <FiEdit2 />
                      )}
                      {editingConfigLoading && editingConfigDataset === d.dataset_id ? 'Loading...' : 'Edit DQ'}
                    </button>
                  </div>
                </motion.div>
              ))
            )}
          </div>
        </div>
      </div>

      <div className="step-footer">
        <button
          className="orch-btn primary step-next-btn"
          onClick={onNext || onRunOrchestration}
          disabled={isOrchestrating}
          style={{ minWidth: 200, fontWeight: 800, fontSize: 0 }}
        >
          <span style={{ fontSize: 14 }}>
            {isOrchestrating ? 'Running...' : (onNext ? 'Continue to Review ->' : 'Run Orchestration')}
          </span>
          {isOrchestrating ? 'Running...' : '🚀 Run Orchestration'}
        </button>
      </div>

      {/* Full Panel Loading Overlay */}
      {(dqLoading || isSuggesting) && (
        <div style={{
          position: 'absolute', top: 0, left: 0, right: 0, bottom: 0,
          background: 'rgba(255,255,255,0.8)', backdropFilter: 'blur(4px)',
          display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
          zIndex: 100, borderRadius: 12
        }}>
          <FiRefreshCw className="spin" style={{ fontSize: 40, color: '#1976d2', marginBottom: 20 }} />
          <h3 style={{ margin: 0, color: '#333' }}>{isSuggesting ? 'Analyzing Dataset & Suggesting Rules...' : 'Loading Configuration...'}</h3>
          <p style={{ margin: '10px 0 0', opacity: 0.6 }}>This may take a moment depending on the dataset size.</p>
        </div>
      )}
    </motion.div>
  );
}
