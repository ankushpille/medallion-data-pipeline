import React, { useState } from 'react';
import { FiLink, FiZap } from 'react-icons/fi';
import { motion } from 'framer-motion';

export default function FabricWorkspace({ onConnected, call, toast }) {
  const [connecting, setConnecting] = useState(false);

  React.useEffect(() => {
    const handleMessage = (event) => {
      // The existing backend flow sends messages with source: 'dea-msal'
      if (event.data.source === 'dea-msal' && event.data.success) {
        if (event.data.target === 'fabric') {
          onConnected(event.data.accessToken);
          toast('Successfully connected to Microsoft Fabric', 'success');
        }
      } else if (event.data.source === 'dea-msal' && !event.data.success) {
        toast('Authentication failed: ' + (event.data.error || 'Unknown error'), 'error');
      }
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [onConnected]);

  const handleConnect = () => {
    const width = 600;
    const height = 700;
    const left = window.screenX + (window.outerWidth - width) / 2;
    const top = window.screenY + (window.outerHeight - height) / 2;
    
    // Using the existing backend SSO flow
    const backendUrl = 'http://localhost:8001';
    const loginUrl = `${backendUrl}/auth/microsoft/login?target=fabric&origin=${encodeURIComponent(window.location.origin)}`;
    
    window.open(
      loginUrl, 
      'FabricLogin', 
      `width=${width},height=${height},left=${left},top=${top}`
    );
    toast('Opening Microsoft Login...', 'info');
  };

  return (
    <motion.div 
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      className="fabric-connect-container"
      style={{ padding: '40px', textAlign: 'center', background: 'rgba(255,255,255,0.05)', borderRadius: '24px', border: '1px solid rgba(255,255,255,0.1)' }}
    >
      <div className="fabric-hero-icon" style={{ fontSize: '48px', marginBottom: '20px', color: '#6366f1' }}>
        <FiZap />
      </div>
      <h3 style={{ marginBottom: '12px', fontWeight: 800 }}>Microsoft Fabric Integration</h3>
      <p style={{ color: 'var(--text3)', marginBottom: '32px', fontSize: '14px' }}>
        Auto-discover pipelines, export configurations, and deploy ZIP definitions directly from the DEA agent.
      </p>
      
      <button 
        className="orch-btn primary premium-btn" 
        onClick={handleConnect}
        style={{ padding: '0 40px', height: '52px' }}
      >
        <FiLink style={{ marginRight: '10px' }} /> SSO Login (Fabric)
      </button>

      <div style={{ marginTop: '30px', paddingTop: '20px', borderTop: '1px solid rgba(255,255,255,0.1)' }}>
         <p style={{ fontSize: '12px', opacity: 0.5 }}>
            This will open a secure Microsoft login popup to retrieve your Fabric Access Token.
         </p>
      </div>
    </motion.div>
  );
}
