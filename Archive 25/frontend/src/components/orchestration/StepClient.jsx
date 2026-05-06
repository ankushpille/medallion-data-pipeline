import { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { FiCheck, FiSearch, FiPlus, FiRefreshCw, FiTrash2, FiClock, FiAlertTriangle, FiX, FiLink, FiBox, FiCloud, FiFolder } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import FluentSelect from '../FluentSelect';
import logo from '../../assets/images/image.png';

const itemVariants = {
  initial: { opacity: 0, y: 10 },
  animate: { opacity: 1, y: 0 }
};

export default function StepClient({ clients, clientLoading, selectedClient, setSelectedClient, fetchClients, onNext, call, toast, sourceForm, setSourceForm, registerSource, savingSource, testConnection, testingConnection, connectionVerified, setConnectionVerified, testResult, extractedFabricData, setExtractedFabricData, onDeploySuccess }) {
  const navigate = useNavigate();
  const [tab, setTab] = useState('existing'); // 'existing' | 'register'
  const [searchQuery, setSearchQuery] = useState('');
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const [clientToDelete, setClientToDelete] = useState(null);


  const filteredClients = (clients || []).filter(c =>
    c.toLowerCase().includes(searchQuery.toLowerCase())
  );
  const apiHasScanDetails = sourceForm.source_type === 'API' && !!sourceForm.base_url && !!String(sourceForm.endpoints || '').trim();
  const apiCanSkipConnection = sourceForm.source_type === 'API' && !apiHasScanDetails;

  const handleSelectClient = (c) => {
    setSelectedClient(c);
    localStorage.setItem('client_name', c);
  };

  const deleteClient = (e, c) => {
    e.stopPropagation();
    setClientToDelete(c);
    setShowDeleteConfirm(true);
  };

  const handleConfirmDelete = async () => {
    if (!clientToDelete) return;
    try {
      await call(`/config/clients/${clientToDelete}`, 'DELETE');
      toast(`Successfully deleted client "${clientToDelete}"`, 'success');
      if (selectedClient === clientToDelete) {
        setSelectedClient(null);
      }
      setShowDeleteConfirm(false);
      setClientToDelete(null);
      fetchClients();
    } catch (err) {
      toast(`Deletion failed: ${err.message}`, 'error');
    }
  };

  return (
    <motion.div
      key="step1"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 24, paddingBottom: 24, borderBottom: '1px solid rgba(0,0,0,0.05)' }}>
        <div style={{ flex: 1 }}>
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900, background: 'linear-gradient(90deg, var(--text1), var(--text2))', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>DEA Agent — Client Setup</h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8, fontSize: 13, fontWeight: 500 }}>Register a new client or choose an existing one to begin.</p>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
          {/* Moved Tab Toggle to Top Right */}
          <div className="step-tabs" style={{ margin: 0, scale: 0.9 }}>
            <button
              className={`step-tab ${tab === 'existing' ? 'active' : ''}`}
              onClick={() => setTab('existing')}
              style={{ padding: '8px 20px' }}
            >
              Choose Existing
            </button>
            <button
              className={`step-tab ${tab === 'register' ? 'active' : ''}`}
              onClick={() => setTab('register')}
              style={{ padding: '8px 20px' }}
            >
              <FiPlus style={{ marginRight: 6 }} /> Register New
            </button>
          </div>

          <div className="header-logo-divider" style={{ width: 1, height: 24, background: 'rgba(0,0,0,0.1)' }} />
          <img src={logo} alt="Agilisium" style={{ height: 28, objectFit: 'contain' }} />
        </div>
      </div>

      {tab === 'existing' && (
        <div className="step-body">
          {/* Search Bar / Actions */}
          <div className="client-search-bar" style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            <div style={{ position: 'relative', flex: 1 }}>
              <FiSearch className="search-icon" style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: 'var(--text2)' }} />
              <input
                type="text"
                placeholder="Search clients..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="search-input"
                style={{ width: '100%', paddingLeft: 40 }}
              />
            </div>

            <button className="orch-btn ghost tiny" onClick={fetchClients} title="Refresh Client List">
              <FiRefreshCw />
            </button>
          </div>

          {/* Client Grid */}
          <div className="client-grid">
            {clientLoading ? (
              Array.from({ length: 8 }).map((_, i) => (
                <div key={i} className="client-card skeleton" aria-hidden>
                  <div className="skeleton-circle" style={{ width: 44, height: 44, borderRadius: 12 }} />
                  <div style={{ flex: 1 }}>
                    <div className="skeleton-line" style={{ width: '70%', height: 14 }} />
                    <div className="skeleton-line" style={{ width: '50%', height: 10, marginTop: 6 }} />
                  </div>
                </div>
              ))
            ) : filteredClients.length === 0 ? (
              <div className="empty-client-state">
                <p>{searchQuery ? `No clients matching "${searchQuery}"` : 'No clients found. Register one first.'}</p>
              </div>
            ) : (
              filteredClients.map((c) => (
                <motion.div
                  key={c}
                  variants={itemVariants}
                  initial="initial"
                  animate="animate"
                  className={`client-card ${selectedClient === c ? 'selected' : ''}`}
                  onClick={() => handleSelectClient(c)}
                  role="button"
                  style={{ position: 'relative' }}
                >
                  <div className="client-avatar">
                    {c.split(/[-_ ]/).map(p => p[0]).slice(0, 2).join('').toUpperCase()}
                  </div>
                  <div style={{ flex: 1 }}>
                    <div className="client-name">{c}</div>
                    <div className="client-env">Azure Environment</div>
                  </div>
                  <div className="client-marker" style={{ position: 'absolute', right: 12, top: 12 }}>
                    <div className="client-card-actions">
                      <button
                        className="delete-client-btn"
                        onClick={(e) => deleteClient(e, c)}
                        title="Delete Client"
                      >
                        <FiTrash2 size={14} />
                      </button>
                    </div>
                    {selectedClient === c && (
                      <div className="selection-badge">
                        <FiCheck size={12} />
                      </div>
                    )}
                  </div>
                </motion.div>
              ))
            )}
          </div>

          {/* Footer Actions - Selected Client Only */}
          <AnimatePresence>
            {selectedClient && (
              <motion.div
                initial={{ opacity: 0, scale: 0.98, y: 10 }}
                animate={{ opacity: 1, scale: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.98, y: 10 }}
                className="step-footer-actions-container"
                style={{
                  display: 'flex',
                  justifyContent: 'flex-end',
                  alignItems: 'center',
                  gap: 12,
                  marginTop: 32,
                  padding: '20px',
                  background: 'rgba(255, 255, 255, 0.4)',
                  backdropFilter: 'blur(10px)',
                  borderRadius: '20px',
                  border: '1px solid rgba(255, 255, 255, 0.5)',
                  boxShadow: '0 8px 32px rgba(0,0,0,0.04)'
                }}
              >
                <button
                  className="orch-btn primary premium-btn"
                  onClick={onNext}
                  style={{
                    height: 48,
                    padding: '0 32px',
                    fontSize: '15px'
                  }}
                >
                  Continue with <strong>{selectedClient}</strong> →
                </button>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      )}

      {tab === 'register' && (
        <div className="step-body">
          <div className="register-form">
            <div className="register-field full" style={{ marginBottom: 20 }}>
              <label>Select Source Type to Register</label>
              <div style={{ display: 'flex', gap: 12, marginTop: 8 }}>
                {[
                  { id: 'LOCAL', label: 'Local Files', icon: <FiFolder />, color: '#10b981', desc: 'Direct upload' },
                  { id: 'API', label: 'REST API', icon: <FiLink />, color: '#3b82f6', desc: 'Secure endpoints' },
                  { id: 'S3', label: 'AWS S3', icon: <FiBox />, color: '#f59e0b', desc: 'Cloud buckets' },
                  { id: 'ADLS', label: 'Azure ADLS', icon: <FiCloud />, color: '#0078d4', desc: 'Azure storage' }
                ].map(t => (
                  <motion.div
                    key={t.id}
                    whileHover={{ y: -4, scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    onClick={() => {
                      setSourceForm({ ...sourceForm, source_type: t.id });
                      setConnectionVerified(false);
                    }}
                    className={`source-type-card ${(sourceForm.source_type || 'API') === t.id ? 'active' : ''}`}
                    style={{
                      '--theme-color': t.color,
                      flex: 1,
                      position: 'relative',
                      cursor: 'pointer'
                    }}
                  >
                    <div className="source-type-icon-wrapper">
                      {t.icon}
                    </div>
                    <div className="source-type-info">
                      <div className="source-type-label">{t.label}</div>
                      <div className="source-type-desc">{t.desc}</div>
                    </div>
                    {(sourceForm.source_type || 'API') === t.id && (
                      <motion.div
                        layoutId="active-glow"
                        className="source-card-glow"
                        initial={false}
                        transition={{ type: "spring", bounce: 0.2, duration: 0.6 }}
                      />
                    )}
                  </motion.div>
                ))}
              </div>
            </div>

            <div className="register-split-grid">
              <div className="register-main-side" style={{ width: '100%' }}>
                <div className="register-field full">
                  <label>Client Name</label>
                  <input
                    value={sourceForm.client_name}
                    onChange={e => {
                      setSourceForm({ ...sourceForm, client_name: e.target.value });
                      setConnectionVerified(false);
                    }}
                    placeholder="e.g. AMGEN"
                  />
                </div>

                {(sourceForm.source_type === 'API' || !sourceForm.source_type) && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="register-grid">
                    <div className="register-field">
                      <label>Source Name</label>
                      <input
                        value={sourceForm.source_name}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, source_name: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="e.g. disease-api"
                      />
                    </div>
                    <div className="register-field">
                      <label>Base URL</label>
                      <input
                        value={sourceForm.base_url}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, base_url: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="https://api.example.com"
                      />
                    </div>
                    <div className="register-field full">
                      <label>Authentication</label>
                      <FluentSelect
                        value={sourceForm.auth_type}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, auth_type: e.target.value });
                          setConnectionVerified(false);
                        }}
                        options={[
                          { value: 'none', label: '🔓 No auth (public API)' },
                          { value: 'bearer', label: '🔑 Bearer token' },
                          { value: 'api_key', label: '🗝️ API key header' },
                          { value: 'basic', label: '🔒 Basic auth' }
                        ]}
                      />
                    </div>
                    {sourceForm.auth_type !== 'none' && (
                      <div className="register-field full">
                        <label>{sourceForm.auth_type === 'api_key' ? 'API Key' : 'Token / Password'}</label>
                        <input
                          type="password"
                          value={sourceForm.auth_token || ''}
                          onChange={e => {
                            setSourceForm({ ...sourceForm, auth_token: e.target.value });
                            setConnectionVerified(false);
                          }}
                          placeholder="Enter credentials..."
                        />
                      </div>
                    )}
                    <div className="register-field full">
                      <label>Endpoints (comma-separated)</label>
                      <input
                        value={sourceForm.endpoints}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, endpoints: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="users,posts,todos"
                      />
                    </div>
                  </motion.div>
                )}

                {sourceForm.source_type === 'S3' && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="register-grid">
                    <div className="register-field">
                      <label>Source Name</label>
                      <input
                        value={sourceForm.source_name}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, source_name: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="e.g. s3-bucket"
                      />
                    </div>
                    <div className="register-field">
                      <label>Bucket Name</label>
                      <input
                        value={sourceForm.bucket_name}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, bucket_name: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="e.g. my-data-bucket"
                      />
                    </div>
                    <div className="register-field">
                      <label>Region</label>
                      <FluentSelect
                        value={sourceForm.region}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, region: e.target.value });
                          setConnectionVerified(false);
                        }}
                        options={[
                          { value: 'us-east-1', label: 'us-east-1 (N. Virginia)' },
                          { value: 'us-east-2', label: 'us-east-2 (Ohio)' },
                          { value: 'us-west-1', label: 'us-west-1 (California)' },
                          { value: 'eu-west-1', label: 'eu-west-1 (Ireland)' }
                        ]}
                      />
                    </div>
                    <div className="register-field">
                      <label>Access Key</label>
                      <input
                        value={sourceForm.aws_access_key_id}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, aws_access_key_id: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="AKIA..."
                      />
                    </div>
                    <div className="register-field full">
                      <label>Secret Key</label>
                      <input
                        type="password"
                        value={sourceForm.aws_secret_access_key}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, aws_secret_access_key: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="Secret..."
                      />
                    </div>
                  </motion.div>
                )}

                {sourceForm.source_type === 'ADLS' && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="register-grid">
                    <div className="register-field">
                      <label>Source Name</label>
                      <input
                        value={sourceForm.source_name}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, source_name: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="e.g. adls-storage"
                      />
                    </div>
                    <div className="register-field">
                      <label>Account Name</label>
                      <input
                        value={sourceForm.azure_account_name}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, azure_account_name: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="mystorageaccount"
                      />
                    </div>
                    <div className="register-field">
                      <label>Container Name</label>
                      <input
                        value={sourceForm.azure_container_name}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, azure_container_name: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="raw-data"
                      />
                    </div>
                    <div className="register-field full">
                      <label>Account Key</label>
                      <input
                        type="password"
                        value={sourceForm.azure_account_key}
                        onChange={e => {
                          setSourceForm({ ...sourceForm, azure_account_key: e.target.value });
                          setConnectionVerified(false);
                        }}
                        placeholder="Azure Key..."
                      />
                    </div>
                  </motion.div>
                )}




                {sourceForm.source_type === 'LOCAL' && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} style={{ padding: '20px', background: 'var(--blue-bg)', borderRadius: 12, marginTop: 12 }}>
                    <div style={{ color: 'var(--blue)', fontWeight: 600, fontSize: 14 }}>
                      💡 For Local sources, just provide the Client Name.
                    </div>
                    <div style={{ color: 'var(--text2)', fontSize: 13, marginTop: 8 }}>
                      You will be prompted to upload your files in the next step.
                    </div>
                  </motion.div>
                )}
              </div>
            </div>

            {testResult && (
              <div style={{
                marginTop: 12,
                padding: '14px 20px',
                borderRadius: 14,
                fontSize: 13,
                fontWeight: 600,
                border: '1px solid',
                background: testResult.type === 'success' ? 'var(--green-bg)' : 'var(--red-bg)',
                borderColor: testResult.type === 'success' ? 'var(--green-bdr)' : 'var(--red-bdr)',
                color: testResult.type === 'success' ? 'var(--green)' : 'var(--red)',
                display: 'flex',
                alignItems: 'center',
                gap: 12
              }}>
                <div style={{ fontSize: 20 }}>{testResult.type === 'success' ? '✅' : '❌'}</div>
                <div>{testResult.message}</div>
              </div>
            )}

            <div className="register-actions">
              {sourceForm.source_type !== 'LOCAL' && (
                <button
                  className="orch-btn ghost"
                  onClick={testConnection}
                  disabled={testingConnection || !sourceForm.client_name || !sourceForm.source_name}
                  style={{ flex: 1 }}
                >
                  {testingConnection ? 'Testing...' : '⚡ Test Connection'}
                </button>
              )}

              <button
                  className={`orch-btn primary premium-btn ${(connectionVerified || sourceForm.source_type === 'LOCAL' || apiCanSkipConnection) ? '' : 'disabled-btn'}`}
                  onClick={() => {
                    if (sourceForm.source_type === 'LOCAL') {
                      setSelectedClient(sourceForm.client_name);
                      toast('Client set. Please upload files in the next step.', 'success');
                      onNext();
                    } else {
                      if (!connectionVerified && !apiCanSkipConnection) {
                        toast('Please test and verify connection before registration', 'warning');
                        return;
                      }
                      registerSource();
                    }
                  }}
                  disabled={savingSource || !sourceForm.client_name || (sourceForm.source_type !== 'LOCAL' && (!sourceForm.source_name || (!connectionVerified && !apiCanSkipConnection)))}
                  style={{ flex: sourceForm.source_type !== 'LOCAL' ? 1.5 : 1 }}
                >
                  {savingSource ? 'Registering...' :
                    sourceForm.source_type === 'LOCAL' ? 'Continue to Upload →' :
                      `🚀 Register ${sourceForm.source_type || 'API'} Source`}
                </button>
            </div>
          </div>
        </div>
      )}

      {/* Portal Confirmation Modal */}
      {showDeleteConfirm && createPortal(
        <div className="mode-modal-overlay" style={{ zIndex: 1200 }}>
          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            animate={{ opacity: 1, scale: 1, transition: { type: "spring", damping: 25, stiffness: 300 } }}
            exit={{ opacity: 0, scale: 0.95 }}
            className="mode-modal-card"
            style={{ width: 440, padding: 0, overflow: 'hidden' }}
          >
            <div className="confirmation-header" style={{ padding: '24px 32px 16px', background: 'var(--surface2)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
                <div style={{ width: 36, height: 36, borderRadius: '50%', background: 'rgba(239, 68, 68, 0.1)', color: '#ef4444', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  <FiAlertTriangle size={20} />
                </div>
                <h3 style={{ margin: 0, fontSize: 18, fontWeight: 900 }}>Confirm Deletion</h3>
              </div>
              <button
                className="orch-btn ghost tiny"
                onClick={() => setShowDeleteConfirm(false)}
                style={{ borderRadius: '50%', width: 32, height: 32, padding: 0 }}
              >
                <FiX />
              </button>
            </div>

            <div className="confirmation-body" style={{ padding: '24px 32px' }}>
              <p style={{ margin: 0, fontSize: 15, fontWeight: 600, color: 'var(--text1)' }}>Are you sure you want to delete client <strong>"{clientToDelete}"</strong>?</p>
              <p style={{ marginTop: 12, fontSize: 13, color: 'var(--text3)', lineHeight: 1.6 }}>
                This action will permanently remove ALL associated sources, datasets, and execution history. <strong style={{ color: '#ef4444' }}>This cannot be undone.</strong>
              </p>
            </div>

            <div className="confirmation-footer" style={{ padding: '20px 32px', background: 'var(--surface2)', display: 'flex', justifyContent: 'flex-end', gap: 12 }}>
              <button
                className="orch-btn ghost"
                onClick={() => setShowDeleteConfirm(false)}
              >
                Cancel
              </button>
              <button
                className="orch-btn primary"
                onClick={handleConfirmDelete}
                style={{ background: '#ef4444', color: '#fff', border: 'none', fontWeight: 800 }}
              >
                <FiTrash2 style={{ marginRight: 8 }} /> Delete Client
              </button>
            </div>
          </motion.div>
        </div>,
        document.body
      )}
    </motion.div>
  );
}
