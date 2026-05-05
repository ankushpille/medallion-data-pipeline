import React, { useState, useEffect } from 'react';
import { FiFolder, FiBox, FiSearch, FiCheck, FiDownload, FiActivity } from 'react-icons/fi';
import { motion, AnimatePresence } from 'framer-motion';

export default function FabricDiscovery({ token, call, toast, onPipelineSelected }) {
  const [workspaces, setWorkspaces] = useState([]);
  const [selectedWorkspace, setSelectedWorkspace] = useState(null);
  const [pipelines, setPipelines] = useState([]);
  const [loading, setLoading] = useState(false);
  const [discoveryLoading, setDiscoveryLoading] = useState(false);

  useEffect(() => {
    fetchWorkspaces();
  }, []);

  const fetchWorkspaces = async () => {
    setLoading(true);
    try {
      const res = await call(`/fabric/workspaces?token=${token}`);
      setWorkspaces(res || []);
    } catch (e) {
      toast('Failed to load workspaces', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleWorkspaceSelect = async (ws) => {
    setSelectedWorkspace(ws);
    setLoading(true);
    try {
      const res = await call(`/fabric/pipelines?workspace_id=${ws.id}&token=${token}`);
      setPipelines(res || []);
    } catch (e) {
      toast('Failed to load pipelines', 'error');
    } finally {
      setLoading(false);
    }
  };

  const handleDiscover = async (pipeline) => {
    setDiscoveryLoading(true);
    try {
      const res = await call(`/fabric/extract?workspace_id=${selectedWorkspace.id}&pipeline_id=${pipeline.id}&token=${token}`, 'POST');
      toast(`Successfully discovered ${pipeline.displayName}`, 'success');
      onPipelineSelected(res);
    } catch (e) {
      toast('Discovery failed', 'error');
    } finally {
      setDiscoveryLoading(false);
    }
  };

  return (
    <div className="fabric-discovery-panel">
      {!selectedWorkspace ? (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
          <div style={{ marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '10px' }}>
            <FiFolder /> <h4 style={{ margin: 0 }}>Select Workspace</h4>
          </div>
          {loading ? (
             <div className="skeleton" style={{ height: '200px', borderRadius: '16px' }} />
          ) : (
            <div className="fabric-grid" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: '16px' }}>
              {Array.isArray(workspaces) ? workspaces.map(ws => (
                <div 
                  key={ws.id} 
                  className="fabric-card" 
                  onClick={() => handleWorkspaceSelect(ws)}
                  style={{ padding: '20px', background: 'rgba(255,255,255,0.05)', borderRadius: '16px', cursor: 'pointer', border: '1px solid rgba(255,255,255,0.1)' }}
                >
                  <div style={{ fontWeight: 700, marginBottom: '4px' }}>{ws.displayName}</div>
                  <div style={{ fontSize: '12px', opacity: 0.6 }}>{ws.type}</div>
                </div>
              )) : (
                <div style={{ colSpan: 'all', textAlign: 'center', padding: '20px', opacity: 0.5 }}>
                  Unable to load workspaces. Please check your connection.
                </div>
              )}
            </div>
          )}
        </motion.div>
      ) : (
        <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
          <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
               <button className="orch-btn ghost tiny" onClick={() => setSelectedWorkspace(null)}>←</button>
               <div>
                  <h4 style={{ margin: 0 }}>{selectedWorkspace.displayName}</h4>
                  <div style={{ fontSize: '12px', opacity: 0.6 }}>Available Pipelines</div>
               </div>
            </div>
            <button className="orch-btn ghost tiny" onClick={fetchWorkspaces}><FiSearch /></button>
          </div>

          {loading ? (
             <div className="skeleton" style={{ height: '300px', borderRadius: '16px' }} />
          ) : (
            <div className="fabric-list" style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              {Array.isArray(pipelines) && pipelines.map(p => (
                <div 
                  key={p.id} 
                  className="fabric-item-row"
                  style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '16px 20px', background: 'rgba(255,255,255,0.03)', borderRadius: '12px', border: '1px solid rgba(255,255,255,0.05)' }}
                >
                  <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                    <div style={{ color: '#6366f1' }}><FiActivity /></div>
                    <div>
                      <div style={{ fontWeight: 600 }}>{p.displayName}</div>
                      <div style={{ fontSize: '11px', opacity: 0.5 }}>{p.id}</div>
                    </div>
                  </div>
                  <button 
                    className="orch-btn primary tiny" 
                    onClick={() => handleDiscover(p)}
                    disabled={discoveryLoading}
                  >
                    {discoveryLoading ? 'Discovery...' : 'Discover'}
                  </button>
                </div>
              ))}
              {(!Array.isArray(pipelines) || pipelines.length === 0) && !loading && (
                <div style={{ textAlign: 'center', padding: '40px', opacity: 0.5 }}>No pipelines found in this workspace</div>
              )}
            </div>
          )}
        </motion.div>
      )}
    </div>
  );
}
