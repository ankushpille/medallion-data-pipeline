import { FiCheck, FiDatabase, FiFile, FiSettings, FiZap } from 'react-icons/fi';
import { motion } from 'framer-motion';
import logo from '../../assets/images/image.png';
import '../PipelineIntelligence.css';

function JsonBlock({ value }) {
  return (
    <pre className="orch-pre" style={{ maxHeight: 220, overflow: 'auto', whiteSpace: 'pre-wrap', fontSize: 12 }}>
      {JSON.stringify(value || {}, null, 2)}
    </pre>
  );
}

function SummaryChip({ label, value }) {
  return (
    <div className="config-chip">
      <strong>{label}:</strong> {value || 'Not selected'}
    </div>
  );
}

export default function StepReviewConfirm({
  selectedClient,
  sourceType,
  folderPath,
  intelligenceData,
  onBack,
  onConfirm,
  isOrchestrating,
}) {
  const executionPayload = {
    endpoint: '/orchestrate/run',
    method: 'POST',
    query: {
      source_type: sourceType,
      client_name: selectedClient,
      folder_path: folderPath,
    },
    intelligence: intelligenceData
      ? {
          framework: intelligenceData.framework,
          target: intelligenceData.ingestion_details?.target,
          auth_mode: intelligenceData.auth_mode,
          scan_status: intelligenceData.scan_status,
          is_fallback: intelligenceData.is_fallback,
          source_path: intelligenceData.ingestion_details?.source_path,
        }
      : null,
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
          <h2 className="step-title" style={{ margin: 0, fontSize: 24, fontWeight: 900 }}>Review & Confirm</h2>
          <p className="step-sub" style={{ margin: '4px 0 0', opacity: 0.8 }}>Validate discovery output and push the run to DEA Agent.</p>
        </div>
        <img src={logo} alt="Agilisium" style={{ height: 32, objectFit: 'contain' }} />
      </div>

      <div className="step-body" style={{ display: 'flex', flexDirection: 'column', gap: 18 }}>
        <div className="dq-config-summary">
          <SummaryChip label="Client" value={selectedClient} />
          <SummaryChip label="Framework" value={intelligenceData?.framework} />
          <SummaryChip label="Auth" value={intelligenceData?.auth_mode} />
          <SummaryChip label="Scan" value={intelligenceData?.scan_status} />
          <SummaryChip label="Fallback" value={intelligenceData?.is_fallback ? 'Yes' : 'No'} />
          <SummaryChip label="Source" value={sourceType} />
          <SummaryChip label="Endpoint" value={folderPath} />
        </div>

        <div className="pi-grid">
          <div className="pi-card">
            <div className="pi-card-title"><FiDatabase /> Ingestion Support by Framework</div>
            <JsonBlock value={intelligenceData?.ingestion_support} />
          </div>
          <div className="pi-card">
            <div className="pi-card-title">Source Systems</div>
            <JsonBlock value={intelligenceData?.source_systems || []} />
          </div>
          <div className="pi-card">
            <div className="pi-card-title"><FiFile /> File Types</div>
            <JsonBlock value={intelligenceData?.file_types || []} />
          </div>
          <div className="pi-card">
            <div className="pi-card-title"><FiSettings /> Delimiters</div>
            <JsonBlock value={intelligenceData?.delimiter_config} />
          </div>
          <div className="pi-card">
            <div className="pi-card-title"><FiZap /> DQ Rules</div>
            <JsonBlock value={intelligenceData?.dq_rules} />
          </div>
        </div>

        <div className="pi-card">
          <div className="pi-card-title">Generated Config</div>
          <JsonBlock value={intelligenceData?.reformatted_config} />
        </div>

        {intelligenceData?.llm_summary && (
          <div className="pi-card">
            <div className="pi-card-title">GPT Summary</div>
            <div className="pi-card-content">{intelligenceData.llm_summary}</div>
          </div>
        )}

        <div className="pi-card">
          <div className="pi-card-title">Execution Payload Preview</div>
          <JsonBlock value={executionPayload} />
        </div>
      </div>

      <div className="step-footer" style={{ justifyContent: 'space-between' }}>
        <button className="orch-btn ghost" onClick={onBack}>Back</button>
        <button
          className="orch-btn primary step-next-btn"
          onClick={onConfirm}
          disabled={isOrchestrating || !selectedClient || !sourceType || !folderPath}
          style={{ minWidth: 240, fontWeight: 800 }}
        >
          <FiCheck style={{ marginRight: 8 }} />
          {isOrchestrating ? 'Pushing...' : 'Confirm & Push to DEA Agent'}
        </button>
      </div>
    </motion.div>
  );
}
