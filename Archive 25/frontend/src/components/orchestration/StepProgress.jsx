import { useState, useEffect, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { FiBarChart2, FiEdit2, FiZap, FiRefreshCw, FiFolder, FiCheckCircle, FiAlertCircle, FiInfo, FiChevronRight, FiChevronUp, FiChevronDown, FiDatabase, FiCloud, FiSettings, FiActivity, FiShield, FiFileText, FiExternalLink, FiEye, FiClock, FiX, FiList } from 'react-icons/fi';
import { useNavigate } from 'react-router-dom';
import { motion, AnimatePresence } from 'framer-motion';
import logo from '../../assets/images/image.png';
import { Handle, Position, MarkerType } from 'reactflow';
import PipelineFlowCanvas from './PipelineFlowCanvas';
import 'reactflow/dist/style.css';

// --- Custom Node Component ---
const PipelineNode = ({ data }) => {
  const isCompleted = data.status?.toUpperCase() === 'PASSED';
  const isRunning = data.status?.toUpperCase() === 'RUNNING';
  const isFailed = ['ERROR', 'FAILED'].includes(data.status?.toUpperCase());
  const isRow2 = data.index >= 4;
  
  const getIcon = (name) => {
    if (name.includes('Source')) return <FiCloud />;
    if (name.includes('Validation')) return <FiShield />;
    if (name.includes('Raw')) return <FiDatabase />;
    if (name.includes('Master')) return <FiSettings />;
    if (name.includes('DQ')) return <FiActivity />;
    return <FiFileText />;
  };

  // Helper to extract path for quick open
  const detail = String(data.detail || '');
  const pathMatch = detail.match(/(?:az:\/\/|Path: |Raw Layer: |key: )[a-zA-Z0-9\-_./() :|%,!@#$+=[\]]+[.]?[a-zA-Z0-9]*/i);
  let extractedPath = pathMatch ? pathMatch[0].replace(/^(Path: |Raw Layer: |key: )/i, '').trim() : null;
  if (extractedPath && extractedPath.includes('|')) {
    extractedPath = extractedPath.split('|')[0].trim();
  }
  if (!extractedPath && detail.includes('az://')) {
    const start = detail.indexOf('az://');
    const end = detail.indexOf('|', start) !== -1 ? detail.indexOf('|', start) : detail.length;
    extractedPath = detail.substring(start, end).trim();
  }

  const isSkeleton = data.isSkeleton;
  
  return (
    <div className={`pipeline-node ${isRunning ? 'running' : ''} ${isCompleted ? 'completed' : ''} ${isFailed ? 'failed' : ''} ${isSkeleton ? 'node-skeleton' : ''}`} onClick={() => !isSkeleton && data.triggerPreview(data.label, extractedPath)}>
      {/* For S-Curve Vertical Connections */}
      <Handle type="target" position={Position.Top} id="top-target" style={{ visibility: 'hidden', pointerEvents: 'none' }} />
      <Handle type="source" position={Position.Bottom} id="bottom-source" style={{ visibility: 'hidden', pointerEvents: 'none' }} />

      {/* Target Handle: Row 1 = Left, Row 2 = Right */}
      <Handle type="target" position={isRow2 ? Position.Right : Position.Left} id="side-target" style={{ visibility: 'hidden', pointerEvents: 'none' }} />
      
      <div className="node-icon">{getIcon(data.label)}</div>
      <div className="node-content">
        <div className="node-label">{data.label}</div>
        <div className="node-status-text">{data.status || 'Pending'}</div>
      </div>
      
      {extractedPath && (
        <div 
          className="node-quick-action" 
          onClick={(e) => { e.stopPropagation(); data.triggerPreview(data.label, extractedPath); }}
          title={`Preview Data: ${extractedPath}`}
        >
          <FiEye />
        </div>
      )}

      {isRunning && <div className="node-glow" />}
      
      {/* Source Handle: Row 1 = Right, Row 2 = Left (except for node index 3, which connects right-to-right) */}
      <Handle type="source" position={isRow2 ? Position.Left : Position.Right} id="side-source" style={{ visibility: 'hidden', pointerEvents: 'none' }} />
    </div>
  );
};

const NODE_TYPES = { pipelineNode: PipelineNode };


export default function StepProgress({
  orchestrateResp, isOrchestrating, loading,
  runOrchestration, loadDqConfig, setPendingDqDataset,
  setShowModeModal, openExplorer, statusColor, call
}) {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState(null);
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [activeDetailTab, setActiveDetailTab] = useState('info'); // 'info' or 'preview'
  const [accumulatedResults, setAccumulatedResults] = useState([]);
  
  // Layer Tabs State
  const [activeLayer, setActiveLayer] = useState(null);
  const [isSummaryExpanded, setIsSummaryExpanded] = useState(false);
  
  // Data Preview State
  const [previewData, setPreviewData] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [showPreviewModal, setShowPreviewModal] = useState(false);
  const isPipelineRunning = isOrchestrating || loading;
  const hasPipelineSummary = (orchestrateResp?.pipeline_results || []).length > 0;

  // Sync and Merge streamed results
  useEffect(() => {
    if (orchestrateResp === null) {
        setAccumulatedResults([]);
        setActiveTab(null);
        return;
    }
    const rawResults = Array.isArray(orchestrateResp) ? orchestrateResp : (orchestrateResp?.progress || []);
    const streamNode = Array.isArray(orchestrateResp) ? null : orchestrateResp?.node;
    
    // If orchestrateResp was reset to empty (Rerun case), clear our internal state.
    // But do NOT clear on terminal summary packets that may omit `progress`.
    if (rawResults.length === 0) {
        const isTerminalSummaryPacket = !Array.isArray(orchestrateResp) && (
          orchestrateResp?.completed === true ||
          orchestrateResp?.status === 'SUCCESS' ||
          (Array.isArray(orchestrateResp?.pipeline_results) && orchestrateResp.pipeline_results.length > 0)
        );

        if (isTerminalSummaryPacket) {
          return;
        }

        setAccumulatedResults([]);
        setActiveTab(null);
        return;
    }

    // A fresh discover event represents a new orchestration run boundary.
    // Reset prior run artifacts so API/S3 source switches don't leak stale tabs/statuses.
    if (streamNode === 'discover') {
      setAccumulatedResults(rawResults);
      setActiveTab(rawResults[0]?.dataset_id || null);
      setSelectedNodeId(null);
      setPreviewData(null);
      setShowPreviewModal(false);
      return;
    }

    setAccumulatedResults(prev => {
        const newMap = new Map();
        // Add existing
        prev.forEach(r => newMap.set(r.dataset_id, r));
        // Merge New (Overwriting with latest)
        rawResults.forEach(r => {
            const existing = newMap.get(r.dataset_id) || {};
            // Deep merge steps
            const mergedSteps = { ...(existing.steps || {}), ...(r.steps || {}) };
            newMap.set(r.dataset_id, { ...existing, ...r, steps: mergedSteps });
        });
        return Array.from(newMap.values());
    });
  }, [orchestrateResp]);

  const results = accumulatedResults;

  // Auto-select first tab when results arrive — ensure activeTab is always valid
  const effectiveActiveTab = activeTab && results.find(r => r.dataset_id === activeTab) ? activeTab : (results[0]?.dataset_id || null);
  useEffect(() => {
    if (effectiveActiveTab && effectiveActiveTab !== activeTab) {
      setActiveTab(effectiveActiveTab);
    }
  }, [effectiveActiveTab, activeTab]);

  // Suppress ResizeObserver loop completed with undelivered notifications error
  useEffect(() => {
    const handleError = (e) => {
      if (e.message === 'ResizeObserver loop completed with undelivered notifications.' || 
          e.message === 'ResizeObserver loop limit exceeded') {
        const resizeObserverErrDiv = document.getElementById('webpack-dev-server-client-overlay-div');
        const resizeObserverErr = document.getElementById('webpack-dev-server-client-overlay');
        if (resizeObserverErr) resizeObserverErr.style.display = 'none';
        if (resizeObserverErrDiv) resizeObserverErrDiv.style.display = 'none';
        e.stopImmediatePropagation();
      }
    };
    window.addEventListener('error', handleError);
    return () => window.removeEventListener('error', handleError);
  }, []);

  const fetchLayerData = async (layerName) => {
    setActiveLayer(layerName);
    const nodeNameMap = { 'Raw': 'Raw Layer', 'Bronze': 'Bronze', 'Silver': 'Silver' };
    const stepName = nodeNameMap[layerName];
    const info = activeResult?.steps?.[stepName];
    if (info?.status !== 'PASSED') {
       setPreviewData(null);
       return;
    }
    
    const detail = String(info.detail || '');
    const pathMatch = detail.match(/(?:az:\/\/|Path: |Raw Layer: |key: )[a-zA-Z0-9\-_./() :|%,!@#$+=[\]]+[.]?[a-zA-Z0-9]*/i);
    let extractedPath = pathMatch ? pathMatch[0].replace(/^(Path: |Raw Layer: |key: )/i, '').trim() : null;
    if (extractedPath && extractedPath.includes('|')) {
      extractedPath = extractedPath.split('|')[0].trim();
    }
    if (!extractedPath && detail.includes('az://')) {
        const start = detail.indexOf('az://');
        const end = detail.indexOf('|', start) !== -1 ? detail.indexOf('|', start) : detail.length;
        extractedPath = detail.substring(start, end).trim();
    }
    
    if (extractedPath) {
      fetchPreview(extractedPath);
    } else {
      setPreviewData(null);
    }
  };
  // Get the result for the active tab
  const activeResult = useMemo(() => {
    if (!effectiveActiveTab) return results[0] || null;
    return results.find(r => r.dataset_id === effectiveActiveTab) || results[0] || null;
  }, [results, effectiveActiveTab]);

  const fetchPreview = useCallback(async (path) => {
    if (!path) return;
    // Strip trailing metrics/info if present
    const cleanPath = path.split('|')[0].trim();
    setPreviewLoading(true);
    setPreviewData({ path: cleanPath, loading: true }); 
    try {
      const res = await call(`/orchestrate/preview?s3_url=${encodeURIComponent(cleanPath)}`);
      setPreviewData(res);
    } catch (e) {
      setPreviewData({ path: cleanPath, error: "No data preview available. The layer might be empty or the file is currently being processed." });
    } finally {
      setPreviewLoading(false);
    }
  }, [call]);

  // Calculate stats for each tab
  const getTabStats = useCallback((ds) => {
    const steps = Object.values(ds?.steps || {});
    if (steps.length === 0) return { passed: 0, error: 0, warning: 0, total: 0 };
    return {
      passed: steps.filter(s => s.status?.toUpperCase() === 'PASSED' || s.status?.toUpperCase() === 'SUCCESS').length,
      error: steps.filter(s => s.status?.toUpperCase() === 'ERROR' || s.status?.toUpperCase() === 'FAILED').length,
      warning: steps.filter(s => s.status?.toUpperCase() === 'WARNING' || s.status?.toUpperCase() === 'WARN').length,
      total: steps.length
    };
  }, []);

  // --- Logic for Node Interaction ---
  const triggerPreview = useCallback((label, path) => {
    setSelectedNodeId(label);
    if (path && ['Raw Layer', 'Master Configuration', 'Bronze', 'Silver'].includes(label)) {
      setShowPreviewModal(true);
      const simpleLabel = label === 'Raw Layer' ? 'Raw' : (label === 'Master Configuration' ? 'Master Configuration' : label);
      setActiveLayer(simpleLabel);
      fetchPreview(path);
    } else {
      setActiveDetailTab('info');
    }
  }, [fetchPreview]);

  // --- React Flow Data Preparation (2-Row S-Curve) ---
  const { nodes, edges } = useMemo(() => {
    const stepsOrdered = [
      'Client Source', 'Validation', 'Raw Layer', 'Master Configuration', 
      'DQ Configuration', 'Bronze', 'Silver', 'Gold'
    ];

    if (!activeResult) {
      // Return skeleton nodes if no real results available yet
      const skeletonNodes = stepsOrdered.map((name, i) => {
        let x, y;
        if (i < 4) { x = i * 300; y = 50; } else { x = (7 - i) * 300; y = 300; }
        return {
          id: name, // Using stable ID for seamless transition
          type: 'pipelineNode',
          data: { label: name, index: i, status: 'PENDING', isSkeleton: true },
          position: { x, y }
        };
      });

      const skeletonEdges = [];
      for (let i = 0; i < stepsOrdered.length - 1; i++) {
        skeletonEdges.push({
          id: `${stepsOrdered[i]}-${stepsOrdered[i+1]}`, // Stable Edge ID
          source: stepsOrdered[i],
          target: stepsOrdered[i+1],
          sourceHandle: 'side-source',
          targetHandle: 'side-target',
          animated: false,
          type: 'smoothstep',
          pathOptions: i === 3 ? { offset: 36, borderRadius: 12 } : { borderRadius: 12 },
          style: { stroke: '#e2e8f0', strokeWidth: 4 },
          markerEnd: { type: MarkerType.ArrowClosed, color: '#e2e8f0' }
        });
      }
      return { nodes: skeletonNodes, edges: skeletonEdges };
    }

    const currentNodes = stepsOrdered.map((name, i) => {
      const info = (activeResult.steps || {})[name] || { status: 'PENDING' };
      
      let x, y;
      if (i < 4) {
        x = i * 300;
        y = 50;
      } else {
        x = (7 - i) * 300;
        y = 300;
      }

      return {
        id: name,
        type: 'pipelineNode',
        data: { label: name, index: i, ...info, openExplorer, triggerPreview },
        position: { x, y },
        className: (selectedNodeId === name) ? 'selected-node' : ''
      };
    });

    const currentEdges = [];
    for (let i = 0; i < stepsOrdered.length - 1; i++) {
        const sourceName = stepsOrdered[i];
        const targetName = stepsOrdered[i+1];
        const sourceInfo = (activeResult.steps || {})[sourceName];
        const targetInfo = (activeResult.steps || {})[targetName];
        
        // --- DOTTED ANIMATED FLOW LOGIC ---
        // A source is "flowing" if it passed/succeeded but the target hasn't passed yet
        const sourcePassed = sourceInfo && ['PASSED', 'SUCCESS'].includes(sourceInfo.status?.toUpperCase());
        const targetPassed = targetInfo && ['PASSED', 'SUCCESS'].includes(targetInfo.status?.toUpperCase());
        const isFlowing = sourcePassed && !targetPassed;
        
        // Edge is animated if flowing
        const isAnimated = isFlowing || (sourceInfo && sourceInfo.status?.toUpperCase() === 'RUNNING');
        const isCompleted = sourcePassed && targetPassed;
        
        currentEdges.push({
            id: `${sourceName}-${targetName}`, // Stable Edge ID
            source: sourceName,
            target: targetName,
          sourceHandle: 'side-source',
          targetHandle: 'side-target',
            animated: isAnimated,
          type: 'smoothstep',
          pathOptions: i === 3 ? { offset: 36, borderRadius: 12 } : { borderRadius: 12 },
            style: { 
                stroke: isCompleted ? '#3b82f6' : (isAnimated ? '#3b82f6' : '#cbd5e1'), 
                strokeWidth: 4, 
                strokeDasharray: isFlowing ? '10 5' : 'none' 
            },
            markerEnd: { type: MarkerType.ArrowClosed, color: isCompleted || isAnimated ? '#3b82f6' : '#cbd5e1' }
        });
    }

    return { nodes: currentNodes, edges: currentEdges };
  }, [activeResult, selectedNodeId, openExplorer, triggerPreview]);

  const onNodeClick = useCallback((event, node) => {
    setSelectedNodeId(node.id);
    setActiveDetailTab('info');
  }, []);

  // --- Detail Panel Logic ---
  const renderDetailPanel = () => {
    if (!selectedNodeId || !activeResult?.steps?.[selectedNodeId]) return null;
    const info = activeResult.steps[selectedNodeId];
    const isFailed = ['ERROR', 'FAILED'].includes(info.status?.toUpperCase());
    const detail = String(info.detail || '');
    
    // Extract Path
    const pathMatch = detail.match(/(?:az:\/\/|Path: |Raw Layer: |key: )[a-zA-Z0-9\-_./() :|%,]+/i);
    let extractedPath = pathMatch ? pathMatch[0].replace(/^(Path: |Raw Layer: |key: )/i, '').trim() : null;
    if (!extractedPath && detail.includes('az://')) {
        const start = detail.indexOf('az://');
        const end = detail.indexOf('|', start) !== -1 ? detail.indexOf('|', start) : detail.length;
        extractedPath = detail.substring(start, end).trim();
    }


    return (
      <motion.div 
        initial={{ opacity: 0, height: 0 }}
        animate={{ opacity: 1, height: 'auto' }}
        className="node-detail-panel"
      >
        <div className="detail-header">
          <div className="detail-tabs">
            <button 
              className="detail-tab active"
            >
              <FiInfo style={{ marginRight: 6 }} /> Step Status
            </button>
          </div>
          <div className={`orch-badge ${isFailed ? 'error' : ''}`} style={{ background: statusColor(info.status) }}>{info.status}</div>
        </div>
        
        <div className="detail-content-area">
          {isFailed && (
            <div className="failure-alert">
               <FiAlertCircle size={20} />
               <div className="failure-msg">
                 <strong>Error:</strong> {detail || 'An unknown error occurred during this step.'}
               </div>
            </div>
          )}

          {!isFailed && extractedPath && (
            <div className="detail-path-card" onClick={() => openExplorer(extractedPath)}>
              <FiFolder className="detail-icon" />
              <div className="detail-path-text">{detail}</div>
              <FiChevronRight className="detail-arrow" />
            </div>
          )}

          {!isFailed && !extractedPath && (
            <div className="detail-info-text">{detail || 'No additional details available for this step.'}</div>
          )}

          {/* Action Buttons */}
          <div className="detail-actions" style={{ marginTop: 24, display: 'flex', gap: 12, borderTop: '1px solid var(--border)', paddingTop: 16 }}>
            {selectedNodeId.includes('DQ') && (
               <button className="orch-btn secondary tiny" onClick={() => loadDqConfig(activeResult.dataset_id)}>
                 <FiEdit2 style={{ marginRight: 6 }} /> Edit DQ Rules
               </button>
            )}
            <button className="orch-btn ghost tiny" onClick={() => openExplorer(extractedPath || '')} disabled={!extractedPath}>
               <FiFolder style={{ marginRight: 6 }} /> Explore Storage
            </button>
          </div>
        </div>
      </motion.div>
    );
  };

  // --- Pipeline Summary Component ---
  const renderSummary = () => {
    const summaryData = orchestrateResp?.pipeline_results || [];
    if (summaryData.length === 0) return null;

    const totalDatasets = summaryData.length;
    const successful = summaryData.filter(r => r.status === 'SUCCESS').length;
    const failed = summaryData.filter(r => r.status === 'FAILURE').length;
    
    // Aggregate metrics
    let totalRowsRead = 0;
    let totalRowsWritten = 0;
    let totalDqViolations = 0;
    
    summaryData.forEach(r => {
      if (r.status === 'SUCCESS' && r.metrics) {
        totalRowsRead += parseInt(r.metrics.raw?.rows_read || 0);
        totalRowsWritten += parseInt(r.metrics.silver?.rows_written || 0);
        
        const dq = r.metrics.dq_details || {};
        totalDqViolations += (dq.violations || []).reduce((acc, v) => acc + parseInt(v.count || 0), 0);
      }
    });

    return (
      <motion.div 
        initial={{ opacity: 0, scale: 0.98 }}
        animate={{ opacity: 1, scale: 1 }}
        className="pipeline-summary-card"
      >
        <div className="summary-header" style={{ borderBottom: isSummaryExpanded ? '1px solid var(--surface2)' : 'none', marginBottom: isSummaryExpanded ? 24 : 0 }}>
          <div className="summary-status-icon">
            <FiCheckCircle size={32} className="success" />
          </div>
          <div className="summary-title-area" style={{ flex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              <h3 className="summary-title" style={{ margin: 0 }}>Pipeline Execution Summary</h3>
              {!isSummaryExpanded && (
                <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                  <span className="tab-pill success" style={{ background: 'rgba(34, 197, 94, 0.1)', color: 'rgb(21, 128, 61)', padding: '2px 8px', borderRadius: 12, fontSize: 12, fontWeight: 600 }}>{successful} Success</span>
                  {failed > 0 && <span className="tab-pill error" style={{ background: 'rgba(239, 68, 68, 0.1)', color: 'rgb(185, 28, 28)', padding: '2px 8px', borderRadius: 12, fontSize: 12, fontWeight: 600 }}>{failed} Failed</span>}
                  <span className="tab-pill" style={{ background: 'var(--surface2)', color: 'var(--text2)', padding: '2px 8px', borderRadius: 12, fontSize: 12, fontWeight: 600 }}>{totalRowsWritten.toLocaleString()} Rows Cleaned</span>
                </div>
              )}
            </div>
            <div className="summary-subtitle" style={{ marginTop: 4 }}>
              Batch completed with <strong>{successful}</strong> success, <strong>{failed}</strong> failures.
            </div>
          </div>
          <div className="summary-actions" style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            <button 
              className="orch-btn secondary" 
              onClick={() => setIsSummaryExpanded(!isSummaryExpanded)}
              style={{
                display: 'flex', alignItems: 'center', gap: 8, 
                background: isSummaryExpanded ? 'rgba(59, 130, 246, 0.08)' : '#f8fafc', 
                color: isSummaryExpanded ? 'var(--blue)' : 'var(--text1)',
                border: isSummaryExpanded ? '1px solid rgba(59, 130, 246, 0.2)' : '1px solid var(--border)',
                fontWeight: 600,
                padding: '8px 16px',
                borderRadius: 8,
                transition: 'all 0.2s ease'
              }}
            >
              {isSummaryExpanded ? <FiChevronUp size={16} /> : <FiList size={16} />}
              {isSummaryExpanded ? 'Collapse Details' : 'Show Full Results'}
            </button>
            <button 
              className="orch-btn primary" 
              onClick={runOrchestration}
              style={{
                display: 'flex', alignItems: 'center', gap: 8,
                background: 'var(--blue)', color: '#fff', border: 'none',
                fontWeight: 600, padding: '8px 20px', borderRadius: 8,
                boxShadow: '0 4px 12px rgba(59, 130, 246, 0.25)'
              }}
            >
              <FiZap size={16} /> Run Again
            </button>
          </div>
        </div>

        <AnimatePresence>
          {isSummaryExpanded && (
            <motion.div
              initial={{ height: 0, opacity: 0 }}
              animate={{ height: 'auto', opacity: 1 }}
              exit={{ height: 0, opacity: 0 }}
              style={{ overflow: 'hidden' }}
            >
              <div className="summary-stats-grid">
                <div className="stat-widget">
                  <div className="stat-label">Total Datasets</div>
                  <div className="stat-value">{totalDatasets}</div>
                </div>
                <div className="stat-widget">
                  <div className="stat-label">Rows Ingested</div>
                  <div className="stat-value">{totalRowsRead.toLocaleString()}</div>
                </div>
                <div className="stat-widget">
                  <div className="stat-label">Clean Rows Produced</div>
                  <div className="stat-value">{totalRowsWritten.toLocaleString()}</div>
                </div>
                <div className="stat-widget highlight">
                  <div className="stat-label">DQ Violations Isolated</div>
                  <div className="stat-value">{totalDqViolations.toLocaleString()}</div>
                </div>
              </div>

              <div className="summary-table-section">
                <h4 className="section-subtitle">Dataset Results</h4>
                <div className="summary-table-wrapper">
                  <table className="summary-table">
                    <thead>
                      <tr>
                        <th>Dataset Name</th>
                        <th>Status</th>
                        <th>Raw Rows</th>
                        <th>Silver Rows</th>
                        <th>DQ Status</th>
                        <th>Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {summaryData.map((res, idx) => {
                        const m = res.metrics || {};
                        const isSuccess = res.status === 'SUCCESS';
                        const dq = m.dq_details || {};
                        const vCount = (dq.violations || []).reduce((acc, v) => acc + parseInt(v.count || 0), 0);
                        const displayName = res.dataset_name || res.dataset_id;
                        
                        return (
                          <tr key={idx}>
                            <td className="font-bold truncate-cell" title={displayName}>
                              {displayName}
                            </td>
                            <td>
                              <span className={`status-pill ${isSuccess ? 'success' : 'error'}`}>
                                {res.status}
                              </span>
                            </td>
                            <td>{isSuccess ? m.raw?.rows_read?.toLocaleString() : '-'}</td>
                            <td>{isSuccess ? m.silver?.rows_written?.toLocaleString() : '-'}</td>
                            <td>
                              {isSuccess ? (
                                <span className={vCount > 0 ? 'text-amber-600' : 'text-green-600'} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                                  {vCount > 0 ? <FiAlertCircle /> : <FiCheckCircle />}
                                  {vCount > 0 ? `${vCount} Issues` : 'Clean'}
                                </span>
                              ) : (
                                <span className="text-red-500" style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                                  <FiAlertCircle /> Failed
                                </span>
                              )}
                            </td>
                            <td>
                              <button 
                                className="orch-btn ghost tiny" 
                                onClick={() => {
                                  setActiveTab(res.dataset_id);
                                  // Scroll to flow diagram
                                  const el = document.querySelector('.execution-tabs-wrapper');
                                  if (el) el.scrollIntoView({ behavior: 'smooth' });
                                }}
                              >
                                <FiEye /> View Flow
                              </button>
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </motion.div>
    );
  };

  return (
    <motion.div
      key="step5"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div className="step-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 30 }}>
        <div>
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900 }}>
            Execution Progress
            {isPipelineRunning && <FiRefreshCw className="spin" style={{ marginLeft: 12, color: 'var(--accent)' }} />}
          </h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8 }}>Real-time pipeline flow visualization.</p>
        </div>
        <img src={logo} alt="Agilisium" style={{ height: 32, objectFit: 'contain' }} />
      </div>

      <div className="step-body" style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
        {/* Utility Bar */}
        <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 20 }}>
          <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
            {isPipelineRunning && (
              <div className="orch-badge" style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: 'rgba(59,130,246,0.12)', color: 'var(--blue)', fontWeight: 700 }}>
                <FiRefreshCw className="spin" /> Streaming...
              </div>
            )}
            <button className="orch-btn ghost tiny" onClick={() => window.location.reload()} title="Reset" style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <FiRefreshCw /> Reset
            </button>
            {!hasPipelineSummary && (
              <button
                className="orch-btn primary"
                onClick={runOrchestration}
                disabled={isPipelineRunning}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  fontWeight: 700,
                  padding: '8px 20px',
                  borderRadius: 8,
                  boxShadow: '0 4px 12px rgba(59, 130, 246, 0.25)'
                }}
              >
                <FiZap size={15} /> {isPipelineRunning ? 'Running...' : 'Run Again'}
              </button>
            )}
          </div>
        </div>
        
        {(!results || results.length === 0) && !isOrchestrating && !loading ? (
          !orchestrateResp?.completed && (
            <div style={{ padding: '40px 24px', textAlign: 'center', background: '#fff', borderRadius: 16, border: '1px solid var(--border)' }}>
              <FiInfo size={48} style={{ opacity: 0.2, marginBottom: 16 }} />
              <h3 style={{ margin: 0, color: 'var(--text1)' }}>No Datasets Discovered</h3>
              <p style={{ fontSize: 13, color: 'var(--text2)', maxWidth: 400, margin: '8px auto' }}>
                  The orchestration completed, but no files were found at the specified source location.
                  Check your folder path or source configuration.
              </p>
              <div style={{ marginTop: 20 }}>
                  <button className="orch-btn ghost tiny" onClick={runOrchestration}>Try Re-running</button>
              </div>
            </div>
          )
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
            {/* Summary appears ABOVE the graph — never replaces it */}
            {orchestrateResp?.completed && renderSummary()}
            
            <div className="execution-tabs-wrapper">
              <div className="execution-tabs">
                {(!results || results.length === 0) ? (
                   <button className="execution-tab active" style={{ opacity: 0.5 }}>
                     <div className="tab-label">Initializing Pipeline...</div>
                   </button>
                ) : results.map((ds) => {
                  const stats = getTabStats(ds);
                  const isActive = effectiveActiveTab === ds.dataset_id;
                  return (
                    <button
                      key={ds.dataset_id}
                      className={`execution-tab ${isActive ? 'active' : ''}`}
                      onClick={() => { setActiveTab(ds.dataset_id); setSelectedNodeId(null); setActiveLayer(null); setPreviewData(null); }}
                      title={ds.dataset_name || ds.dataset_id}
                    >
                      <div className="tab-label">
                        {ds.dataset_name || ds.dataset_id}
                      </div>
                      <div className="tab-badges">
                        {stats.error > 0 && <span className="tab-pill error">{stats.error}</span>}
                        {stats.passed > 0 && <span className="tab-pill success">{stats.passed}</span>}
                      </div>
                      {isActive && <motion.div layoutId="active-tab-line" className="active-tab-line" />}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Isolated canvas component keeps ReactFlow lifecycle stable during stream updates */}
            <PipelineFlowCanvas
              nodes={nodes}
              edges={edges}
              nodeTypes={NODE_TYPES}
              onNodeClick={onNodeClick}
              isStreaming={isPipelineRunning}
            />

            <AnimatePresence>
              {renderDetailPanel()}
            </AnimatePresence>
          </div>
        )}
      </div>

      {/* Data Preview Modal Portal */}
      {showPreviewModal && createPortal(
        <div className="mode-modal-overlay" style={{ zIndex: 1300 }}>
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 20 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 20 }}
            className="preview-modal-card"
            style={{ 
              width: '95vw', 
              height: '95vh', 
              background: '#fff', 
              borderRadius: 24,
              display: 'flex',
              flexDirection: 'column',
              boxShadow: '0 24px 48px rgba(0,0,0,0.1)',
              overflow: 'hidden'
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '24px 32px', borderBottom: '1px solid var(--border)' }}>
              <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
                <div style={{ width: 44, height: 44, borderRadius: 10, background: 'rgba(59,130,246,0.08)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 20 }}>
                  <FiDatabase style={{ color: 'var(--blue)' }} />
                </div>
                <div>
                  <h3 style={{ margin: 0, fontSize: 18 }}>Data Preview: {selectedNodeId}</h3>
                  <div className="step-sub" style={{ marginTop: 4 }}>{previewData?.path || previewData?.error ? 'Preview' : 'Loading data visualization...'}</div>
                </div>
              </div>
              <button className="orch-btn ghost tiny" onClick={() => setShowPreviewModal(false)}>
                <FiX style={{ marginRight: 6 }} size={16} /> Close
              </button>
            </div>
            
            <div style={{ flex: 1, overflow: 'auto', padding: 24, background: '#f8fafc' }}>
               {previewLoading ? (
                  <div className="table-container" style={{ background: '#fff', borderRadius: 16, border: '1px solid var(--border)', overflow: 'hidden' }}>
                    <table className="summary-table">
                      <thead>
                        <tr>
                          {[1,2,3,4,5,6].map(i => <th key={i}><div className="shimmer-block" style={{ width: '60%', height: 16, borderRadius: 4 }} /></th>)}
                        </tr>
                      </thead>
                      <tbody>
                        {[1,2,3,4,5,6,7,8,9,10,11,12].map(row => (
                          <tr key={row}>
                            {[1,2,3,4,5,6].map(col => (
                              <td key={col}><div className="shimmer-block" style={{ width: `${Math.random() * 40 + 40}%`, height: 14, borderRadius: 4 }} /></td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : previewData?.error ? (
                  <div style={{ padding: '80px 0', textAlign: 'center' }}>
                     <FiAlertCircle size={48} style={{ color: 'var(--red)', marginBottom: 16 }} />
                     <p style={{ color: 'var(--text2)', fontSize: 15 }}>{previewData.error}</p>
                  </div>
                ) : !previewData?.rows ? (
                  <div style={{ padding: '80px 0', textAlign: 'center' }}>
                     <FiInfo size={48} style={{ color: 'var(--text3)', marginBottom: 16 }} />
                     <p style={{ color: 'var(--text2)', fontSize: 15 }}>No sample data found for this layer.</p>
                  </div>
                ) : (
                  <div className="table-container" style={{ background: '#fff', borderRadius: 16, border: '1px solid var(--border)', overflow: 'hidden' }}>
                    <table className="summary-table">
                      <thead>
                        <tr>{previewData.columns.map(c => <th key={c}>{c}</th>)}</tr>
                      </thead>
                      <tbody>
                        {previewData.rows.map((row, i) => (
                          <tr key={i}>{previewData.columns.map(c => (
                            <td key={c}>
                              <div className="truncate-cell" title={row[c] !== null ? String(row[c]) : '-'}>
                                {row[c] !== null ? String(row[c]) : '-'}
                              </div>
                            </td>
                          ))}</tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
            </div>
          </motion.div>
        </div>,
        document.body
      )}
    </motion.div>
  );
}
