import { useState } from 'react';
import { FiZap, FiCloud, FiBox, FiCpu, FiCheck, FiChevronRight } from 'react-icons/fi';
import { motion, AnimatePresence } from 'framer-motion';
import logo from '../../assets/images/image.png';

const PLATFORMS = [
  {
    id: 'FABRIC',
    label: 'Microsoft Fabric',
    icon: <FiZap />,
    color: '#6366f1',
    gradient: 'linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%)',
    desc: 'Unified analytics platform',
    features: ['Lakehouse', 'Data Pipelines', 'Notebooks', 'Warehouse'],
  },
  {
    id: 'AZURE',
    label: 'Azure',
    icon: <FiCloud />,
    color: '#0078d4',
    gradient: 'linear-gradient(135deg, #0078d4 0%, #00a4ef 100%)',
    desc: 'Azure Data Factory & ADLS',
    features: ['Data Factory', 'ADLS Gen2', 'Synapse', 'Databricks'],
  },
  {
    id: 'AWS',
    label: 'AWS',
    icon: <FiBox />,
    color: '#f59e0b',
    gradient: 'linear-gradient(135deg, #f59e0b 0%, #f97316 100%)',
    desc: 'S3, Glue & Lambda',
    features: ['S3', 'Glue', 'Lambda', 'Redshift'],
  },
  {
    id: 'DATABRICKS',
    label: 'Databricks',
    icon: <FiCpu />,
    color: '#ef4444',
    gradient: 'linear-gradient(135deg, #ef4444 0%, #f97316 100%)',
    desc: 'Lakehouse platform',
    features: ['Delta Lake', 'Spark', 'MLflow', 'Unity Catalog'],
  },
];

const itemVariants = {
  initial: { opacity: 0, y: 20, scale: 0.95 },
  animate: (i) => ({
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { delay: i * 0.08, type: 'spring', damping: 20, stiffness: 200 },
  }),
};

export default function StepPlatform({ selectedPlatform, setSelectedPlatform, onNext }) {
  return (
    <motion.div
      key="step0"
      initial={{ opacity: 0, x: 20 }}
      animate={{ opacity: 1, x: 0, transition: { duration: 0.4 } }}
      exit={{ opacity: 0, x: -20 }}
      className="orch-step-panel"
    >
      <div
        className="step-header"
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 24,
          paddingBottom: 24,
          borderBottom: '1px solid rgba(0,0,0,0.05)',
        }}
      >
        <div style={{ flex: 1 }}>
          <h2
            className="step-title"
            style={{
              margin: 0,
              fontSize: 24,
              fontWeight: 900,
              background: 'linear-gradient(90deg, var(--text1), var(--text2))',
              WebkitBackgroundClip: 'text',
              WebkitTextFillColor: 'transparent',
            }}
          >
            Select Execution Platform
          </h2>
          <p
            className="step-sub"
            style={{ margin: '4px 0 0', opacity: 0.8, fontSize: 13, fontWeight: 500 }}
          >
            Choose the hyperscaler or platform where your pipelines run. This determines discovery agents, deployment targets, and available integrations.
          </p>
        </div>
        <img src={logo} alt="Agilisium" style={{ height: 28, objectFit: 'contain' }} />
      </div>

      <div className="step-body">
        <div
          className="platform-grid"
          style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
            gap: 20,
          }}
        >
          {PLATFORMS.map((p, i) => (
            <motion.div
              key={p.id}
              custom={i}
              variants={itemVariants}
              initial="initial"
              animate="animate"
              whileHover={{ y: -6, scale: 1.02, transition: { duration: 0.2 } }}
              whileTap={{ scale: 0.98 }}
              onClick={() => setSelectedPlatform(p.id)}
              style={{
                position: 'relative',
                cursor: 'pointer',
                padding: 28,
                borderRadius: 20,
                background: selectedPlatform === p.id
                  ? `linear-gradient(135deg, ${p.color}08 0%, ${p.color}15 100%)`
                  : 'rgba(255,255,255,0.6)',
                border: `2px solid ${selectedPlatform === p.id ? p.color : 'rgba(0,0,0,0.06)'}`,
                boxShadow: selectedPlatform === p.id
                  ? `0 8px 32px ${p.color}20, 0 2px 8px rgba(0,0,0,0.04)`
                  : '0 2px 12px rgba(0,0,0,0.03)',
                transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                overflow: 'hidden',
              }}
            >
              {/* Selection indicator */}
              {selectedPlatform === p.id && (
                <motion.div
                  initial={{ scale: 0 }}
                  animate={{ scale: 1 }}
                  style={{
                    position: 'absolute',
                    top: 14,
                    right: 14,
                    width: 28,
                    height: 28,
                    borderRadius: '50%',
                    background: p.gradient,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    color: '#fff',
                    boxShadow: `0 4px 12px ${p.color}40`,
                  }}
                >
                  <FiCheck size={14} />
                </motion.div>
              )}

              {/* Icon */}
              <div
                style={{
                  width: 52,
                  height: 52,
                  borderRadius: 16,
                  background: p.gradient,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  color: '#fff',
                  fontSize: 24,
                  marginBottom: 18,
                  boxShadow: `0 6px 20px ${p.color}30`,
                }}
              >
                {p.icon}
              </div>

              {/* Label */}
              <div
                style={{
                  fontSize: 18,
                  fontWeight: 800,
                  color: selectedPlatform === p.id ? p.color : 'var(--text1)',
                  marginBottom: 6,
                  transition: 'color 0.2s',
                }}
              >
                {p.label}
              </div>

              {/* Description */}
              <div
                style={{
                  fontSize: 13,
                  color: 'var(--text3)',
                  marginBottom: 16,
                  fontWeight: 500,
                }}
              >
                {p.desc}
              </div>

              {/* Feature pills */}
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {p.features.map((f) => (
                  <span
                    key={f}
                    style={{
                      fontSize: 10,
                      fontWeight: 700,
                      padding: '4px 10px',
                      borderRadius: 20,
                      background: selectedPlatform === p.id ? `${p.color}15` : 'var(--surface2)',
                      color: selectedPlatform === p.id ? p.color : 'var(--text3)',
                      transition: 'all 0.2s',
                      letterSpacing: '0.02em',
                    }}
                  >
                    {f}
                  </span>
                ))}
              </div>
            </motion.div>
          ))}
        </div>
      </div>

      {/* Footer */}
      <AnimatePresence>
        {selectedPlatform && (
          <motion.div
            initial={{ opacity: 0, scale: 0.98, y: 10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.98, y: 10 }}
            className="step-footer-actions-container"
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginTop: 32,
              padding: '20px 24px',
              background: 'rgba(255, 255, 255, 0.4)',
              backdropFilter: 'blur(10px)',
              borderRadius: 20,
              border: '1px solid rgba(255, 255, 255, 0.5)',
              boxShadow: '0 8px 32px rgba(0,0,0,0.04)',
            }}
          >
            <div style={{ fontSize: 13, fontWeight: 600, color: 'var(--text2)' }}>
              Selected: <strong style={{ color: PLATFORMS.find((p) => p.id === selectedPlatform)?.color }}>
                {PLATFORMS.find((p) => p.id === selectedPlatform)?.label}
              </strong>
            </div>
            <button
              className="orch-btn primary premium-btn"
              onClick={onNext}
              style={{ height: 48, padding: '0 32px', fontSize: '15px', display: 'flex', alignItems: 'center', gap: 8 }}
            >
              Continue <FiChevronRight />
            </button>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
