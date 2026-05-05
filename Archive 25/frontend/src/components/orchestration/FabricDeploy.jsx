import React, { useState } from 'react';
import { FiUploadCloud, FiCheck, FiAlertCircle } from 'react-icons/fi';
import { motion } from 'framer-motion';

export default function FabricDeploy({ token, call, toast, onDeploySuccess }) {
  const [file, setFile] = useState(null);
  const [pipelineName, setPipelineName] = useState('');
  const [workspaceId, setWorkspaceId] = useState('');
  const [deploying, setDeploying] = useState(false);

  const handleDeploy = async () => {
    if (!file || !pipelineName || !workspaceId) {
      toast('Please fill all fields and select a file', 'warning');
      return;
    }

    setDeploying(true);
    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('pipeline_name', pipelineName);
      formData.append('workspace_id', workspaceId);
      formData.append('token', token);

      const res = await fetch('/fabric/deploy', {
        method: 'POST',
        body: formData,
      });
      
      if (!res.ok) throw new Error('Deployment failed');
      
      const data = await res.json();
      toast('Pipeline deployed successfully!', 'success');
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
            placeholder="Enter Workspace GUID" 
            value={workspaceId}
            onChange={(e) => setWorkspaceId(e.target.value)}
          />
        </div>

        <div className="form-group">
          <label style={{ fontSize: '13px', fontWeight: 600, marginBottom: '8px', display: 'block' }}>New Pipeline Name</label>
          <input 
            className="orch-input" 
            placeholder="Enter Name" 
            value={pipelineName}
            onChange={(e) => setPipelineName(e.target.value)}
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
          disabled={deploying || !file}
          style={{ marginTop: '10px' }}
        >
          {deploying ? 'Deploying...' : 'Start Deployment'}
        </button>
      </div>
    </motion.div>
  );
}
