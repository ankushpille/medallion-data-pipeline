import { useMemo, useState } from 'react';
import { FiCloud, FiLock, FiSearch, FiX, FiZap } from 'react-icons/fi';
import { motion } from 'framer-motion';
import { apiUrl } from '../../hooks/useApi';

const PORTALS = [
  { id: 'aws', label: 'AWS', authMode: 'credentials', icon: <FiCloud /> },
  { id: 'azure', label: 'Azure', authMode: 'credentials', icon: <FiCloud /> },
  { id: 'fabric', label: 'Microsoft Fabric', authMode: 'sso', icon: <FiZap /> },
];

const EMPTY_CREDS = {
  aws: { access_key: '', secret_key: '', region: '', role_arn: '' },
  azure: { tenant_id: '', client_id: '', client_secret: '', subscription_id: '', resource_group: '' },
  fabric: { sso_token: '' },
};

function CredentialInput({ label, value, onChange, type = 'text', placeholder = '' }) {
  return (
    <label className="cloud-scan-field">
      <span>{label}</span>
      <input
        className="orch-input"
        type={type}
        value={value}
        placeholder={placeholder}
        autoComplete="off"
        onChange={(e) => onChange(e.target.value)}
      />
    </label>
  );
}

export default function CloudPortalScanModal({ selectedClient, initialTarget = 'aws', useCloudLlm = true, onTargetChange, onClose, onScanComplete }) {
  const [target, setTarget] = useState(initialTarget);
  const [credentials, setCredentials] = useState(EMPTY_CREDS);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const selectedPortal = useMemo(() => PORTALS.find((p) => p.id === target), [target]);

  const updateCredential = (key, value) => {
    setCredentials((prev) => ({
      ...prev,
      [target]: { ...(prev[target] || {}), [key]: value },
    }));
  };

  const buildCredentials = () => {
    const raw = credentials[target] || {};
    return Object.fromEntries(Object.entries(raw).filter(([, value]) => String(value || '').trim() !== ''));
  };

  const runScan = async () => {
    if (!selectedClient) {
      setError('Select a client before scanning a cloud framework.');
      return;
    }

    setLoading(true);
    setError('');
    try {
      const requestCredentials = buildCredentials();
      const hasCredentials = Object.keys(requestCredentials).length > 0;
      let authMode = selectedPortal?.authMode || 'credentials';
      const headers = { 'Content-Type': 'application/json' };
      if (target === 'fabric' && requestCredentials.sso_token) {
        headers.Authorization = `Bearer ${requestCredentials.sso_token}`;
        delete requestCredentials.sso_token;
      }
      if (!hasCredentials && target !== 'fabric') authMode = 'none';
      if (target === 'fabric' && !hasCredentials) authMode = 'none';
      const hasAwsKeys = target !== 'aws' || (requestCredentials.access_key && requestCredentials.secret_key);
      const scanMode = hasCredentials && hasAwsKeys ? 'real' : 'mock';

      const response = await fetch(apiUrl('/discovery/analyze'), {
        method: 'POST',
        headers,
        body: JSON.stringify({
          client_name: selectedClient,
          target,
          scan_mode: scanMode,
          auth_mode: authMode,
          credentials: requestCredentials,
          use_cloud_llm: useCloudLlm,
          llm_provider: 'gpt',
        }),
      });

      if (!response.ok) throw new Error(`Scan failed with status ${response.status}`);
      const data = await response.json();
      onScanComplete(data);
      setCredentials(EMPTY_CREDS);
      onClose();
    } catch (scanError) {
      setError(scanError?.message || 'Framework scan failed.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="mode-modal-overlay" style={{ zIndex: 1400 }}>
      <motion.div
        initial={{ opacity: 0, scale: 0.96 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.96 }}
        className="mode-modal-card cloud-scan-modal"
      >
        <div className="cloud-scan-header">
          <div>
            <h3 style={{ margin: 0 }}>Cloud Portal Selection</h3>
            <div className="step-sub" style={{ marginTop: 4 }}>Run framework discovery with transient scan credentials.</div>
          </div>
          <button className="orch-btn ghost tiny" onClick={onClose} aria-label="Close"><FiX /></button>
        </div>

        <div className="cloud-scan-portals">
          {PORTALS.map((portal) => (
            <button
              key={portal.id}
              type="button"
              className={`cloud-scan-portal ${target === portal.id ? 'selected' : ''}`}
              onClick={() => {
                setTarget(portal.id);
                onTargetChange?.(portal.id);
              }}
            >
              <span>{portal.icon}</span>
              {portal.label}
            </button>
          ))}
        </div>

        <div className="cloud-scan-body">
          {target === 'aws' && (
            <div className="cloud-scan-form">
              <CredentialInput label="Access Key" value={credentials.aws.access_key} onChange={(v) => updateCredential('access_key', v)} />
              <CredentialInput label="Secret Key" type="password" value={credentials.aws.secret_key} onChange={(v) => updateCredential('secret_key', v)} />
              <CredentialInput label="Region" value={credentials.aws.region} onChange={(v) => updateCredential('region', v)} placeholder="us-east-1" />
              <CredentialInput label="Role ARN" value={credentials.aws.role_arn} onChange={(v) => updateCredential('role_arn', v)} />
            </div>
          )}

          {target === 'azure' && (
            <div className="cloud-scan-form">
              <CredentialInput label="Tenant ID" value={credentials.azure.tenant_id} onChange={(v) => updateCredential('tenant_id', v)} />
              <CredentialInput label="Client ID" value={credentials.azure.client_id} onChange={(v) => updateCredential('client_id', v)} />
              <CredentialInput label="Client Secret" type="password" value={credentials.azure.client_secret} onChange={(v) => updateCredential('client_secret', v)} />
              <CredentialInput label="Subscription ID" value={credentials.azure.subscription_id} onChange={(v) => updateCredential('subscription_id', v)} />
              <CredentialInput label="Resource Group" value={credentials.azure.resource_group} onChange={(v) => updateCredential('resource_group', v)} />
            </div>
          )}

          {target === 'fabric' && (
            <div className="cloud-scan-sso">
              <button className="orch-btn primary" type="button">
                <FiLock style={{ marginRight: 8 }} /> Sign in with Microsoft / Continue with SSO
              </button>
              <div className="step-sub">
                SSO is pluggable here. For local demos, paste an existing Fabric/Azure bearer token if available.
              </div>
              <CredentialInput label="Demo SSO Token" type="password" value={credentials.fabric.sso_token} onChange={(v) => updateCredential('sso_token', v)} />
            </div>
          )}
        </div>

        {error && <div className="panel-error-alert">{error}</div>}

        <div className="step-footer" style={{ marginTop: 18, paddingTop: 18 }}>
          <button className="orch-btn ghost" onClick={onClose} disabled={loading}>Cancel</button>
          <button className="orch-btn primary" onClick={runScan} disabled={loading || !selectedClient}>
            <FiSearch style={{ marginRight: 8 }} />
            {loading ? 'Scanning Framework...' : 'Run Framework Scan'}
          </button>
        </div>
      </motion.div>
    </div>
  );
}
