import React, { useState, useEffect } from 'react';
import { FiUploadCloud, FiCheck, FiAlertCircle } from 'react-icons/fi';
import { motion } from 'framer-motion';

export default function FabricDeploy({ token, call, toast, onDeploySuccess, selectedWorkspace }) {
  const [file, setFile] = useState(null);
  const [workspaceId, setWorkspaceId] = useState('');
  const [deploying, setDeploying] = useState(false);

  // Auto-fill Workspace ID when selectedWorkspace prop changes
  useEffect(() => {
    if (selectedWorkspace) {
      setWorkspaceId(selectedWorkspace.id);
    }
  }, [selectedWorkspace]);

  const API_BASE = process.env.REACT_APP_API_URL || "http://127.0.0.1:8001";

  const handleDeploy = async () => {
    if (!workspaceId || !file) {
      toast('Please select workspace and upload file', 'warning');
      return;
    }

    setDeploying(true);
    try {
      const formData = new FormData();
      formData.append('zip_file', file);
      formData.append('target_workspace_id', workspaceId);
      formData.append('access_token', token);

      const res = await fetch(`${API_BASE}/deploy/execute`, {
        method: 'POST',
        body: formData,
      });
      
      if (!res.ok) {
        const errData = await res.json();
        throw new Error(errData.detail || 'Deployment failed');
      }
      
      const data = await res.json();
      toast(`Deployed: ${data.pipeline_deployed}`, 'success');
      if (onDeploySuccess) onDeploySuccess(data);
    } catch (e) {
      toast('Deployment error: ' + e.message, 'error');
    } finally {
      setDeploying(false);
    }
  };

  return (
    <motion.div 
      initial={{ opacity: 0 }} 
      animate={{ opacity: 1 }}
      className="fabric-deploy-panel"
      style={{ padding: '24px', background: 'rgba(255,255,255,0.03)', borderRadius: '20px', border: '1px solid rgba(255,255,255,0.05)' }}
    >
      <div style={{ marginBottom: '24px' }}>
        <h4 style={{ margin: 0 }}>Deploy Pipeline ZIP</h4>
        <p style={{ fontSize: '12px', opacity: 0.6 }}>Upload a exported pipeline definition to Fabric</p>
      </div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
        <div className="form-group">
          <label style={{ fontSize: '13px', fontWeight: 600, marginBottom: '8px', display: 'block' }}>Target Workspace ID</label>
          <input 
            className="orch-input" 
            placeholder="Select a workspace first" 
            value={workspaceId}
            readOnly
            style={{ opacity: 0.8, cursor: 'not-allowed' }}
          />
        </div>

        <div 
          className="upload-dropzone" 
          style={{ padding: '30px', textAlign: 'center', border: '2px dashed rgba(255,255,255,0.1)', borderRadius: '12px' }}
        >
          <input 
            type="file" 
            id="fabric-deploy-file" 
            style={{ display: 'none' }} 
            onChange={(e) => setFile(e.target.files[0])}
          />
          <label htmlFor="fabric-deploy-file" style={{ cursor: 'pointer' }}>
            <FiUploadCloud size={32} style={{ marginBottom: '10px', color: '#6366f1' }} />
            <div style={{ fontWeight: 600 }}>{file ? file.name : 'Select Pipeline JSON / ZIP'}</div>
          </label>
        </div>

        <button 
          className="orch-btn primary" 
          onClick={handleDeploy}
          disabled={deploying || !file || !workspaceId}
          style={{ marginTop: '10px' }}
        >
          {deploying ? 'Deploying...' : 'Start Deployment'}
        </button>
      </div>
    </motion.div>
  );
}
