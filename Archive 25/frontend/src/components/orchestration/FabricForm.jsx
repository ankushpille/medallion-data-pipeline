import React, { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { FiCloud, FiCpu, FiKey, FiShield, FiDatabase, FiDownload, FiCheckCircle, FiAlertCircle, FiLoader } from 'react-icons/fi';

export default function FabricForm({ onExtractSuccess, call, toast }) {
    const [formData, setFormData] = useState({
        workspace_id: '',
        pipeline_id: '',
        client_id: '',
        client_secret: '',
        tenant_id: ''
    });
    const [loading, setLoading] = useState(false);
    const [result, setResult] = useState(null);

    const handleChange = (e) => {
        setFormData({ ...formData, [e.target.name]: e.target.value });
    };

    const handleSubmit = async (e) => {
        e.preventDefault();
        setLoading(true);
        setResult(null);

        try {
            const data = await call('/fabric/extract', 'POST', formData);
            setResult(data);
            toast?.("Pipeline extracted successfully!", "success");
            onExtractSuccess?.(data);
        } catch (error) {
            console.error("Fabric extraction failed:", error);
            toast?.(error.message || "Failed to extract Fabric pipeline", "error");
        } finally {
            setLoading(false);
        }
    };

    const downloadJson = (data, filename) => {
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    };

    return (
        <div className="fabric-form-container">
            <form onSubmit={handleSubmit} className="fabric-glass-form">
                <div className="form-grid">
                    <div className="form-group">
                        <label><FiDatabase /> Workspace ID</label>
                        <input
                            type="text"
                            name="workspace_id"
                            value={formData.workspace_id}
                            onChange={handleChange}
                            required
                            placeholder="Enter Fabric Workspace GUID"
                            className="orch-input"
                        />
                    </div>
                    <div className="form-group">
                        <label><FiCpu /> Pipeline (Item) ID</label>
                        <input
                            type="text"
                            name="pipeline_id"
                            value={formData.pipeline_id}
                            onChange={handleChange}
                            required
                            placeholder="Enter Pipeline Item GUID"
                            className="orch-input"
                        />
                    </div>
                    <div className="form-group">
                        <label><FiKey /> Client ID</label>
                        <input
                            type="text"
                            name="client_id"
                            value={formData.client_id}
                            onChange={handleChange}
                            required
                            placeholder="Azure App Registration Client ID"
                            className="orch-input"
                        />
                    </div>
                    <div className="form-group">
                        <label><FiShield /> Client Secret</label>
                        <input
                            type="password"
                            name="client_secret"
                            value={formData.client_secret}
                            onChange={handleChange}
                            required
                            placeholder="Azure App Registration Secret"
                            className="orch-input"
                        />
                    </div>
                    <div className="form-group full-width">
                        <label><FiCloud /> Tenant ID</label>
                        <input
                            type="text"
                            name="tenant_id"
                            value={formData.tenant_id}
                            onChange={handleChange}
                            required
                            placeholder="Azure Tenant GUID"
                            className="orch-input"
                        />
                    </div>
                </div>

                <div className="form-actions">
                    <button 
                        type="submit" 
                        className={`orch-btn primary ${loading ? 'loading' : ''}`}
                        disabled={loading}
                    >
                        {loading ? (
                            <><FiLoader className="spin" /> Extracting...</>
                        ) : (
                            <><FiDownload /> Register + Extract Pipeline</>
                        )}
                    </button>
                </div>
            </form>

            <AnimatePresence>
                {result && (
                    <motion.div 
                        initial={{ opacity: 0, y: 10 }}
                        animate={{ opacity: 1, y: 0 }}
                        className="extraction-results"
                    >
                        <div className="result-header">
                            <FiCheckCircle color="#10b981" size={24} />
                            <h3>Extraction Complete</h3>
                        </div>
                        <div className="result-actions">
                            <button 
                                className="orch-btn secondary tiny"
                                onClick={() => downloadJson(result.pipeline_json, 'pipeline.json')}
                            >
                                <FiDownload /> Download pipeline.json
                            </button>
                            <button 
                                className="orch-btn secondary tiny"
                                onClick={() => downloadJson(result.manifest_json, 'manifest.json')}
                            >
                                <FiDownload /> Download manifest.json
                            </button>
                        </div>
                    </motion.div>
                )}
            </AnimatePresence>

            <style jsx>{`
                .fabric-form-container {
                    padding: 10px 0;
                }
                .fabric-glass-form {
                    background: rgba(255, 255, 255, 0.05);
                    backdrop-filter: blur(10px);
                    border: 1px solid rgba(255, 255, 255, 0.1);
                    border-radius: 16px;
                    padding: 24px;
                    margin-bottom: 20px;
                }
                .form-grid {
                    display: grid;
                    grid-template-columns: 1fr 1fr;
                    gap: 20px;
                }
                .form-group {
                    display: flex;
                    flex-direction: column;
                    gap: 8px;
                }
                .form-group.full-width {
                    grid-column: span 2;
                }
                .form-group label {
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    font-size: 13px;
                    font-weight: 600;
                    color: var(--text2);
                }
                .form-actions {
                    margin-top: 24px;
                    display: flex;
                    justify-content: flex-end;
                }
                .extraction-results {
                    background: rgba(16, 185, 129, 0.1);
                    border: 1px solid rgba(16, 185, 129, 0.2);
                    border-radius: 12px;
                    padding: 20px;
                    display: flex;
                    flex-direction: column;
                    gap: 16px;
                }
                .result-header {
                    display: flex;
                    align-items: center;
                    gap: 12px;
                }
                .result-header h3 {
                    margin: 0;
                    font-size: 16px;
                    color: #10b981;
                }
                .result-actions {
                    display: flex;
                    gap: 12px;
                }
                .spin {
                    animation: spin 1s linear infinite;
                }
                @keyframes spin {
                    from { transform: rotate(0deg); }
                    to { transform: rotate(360deg); }
                }
            `}</style>
        </div>
    );
}
