import { BrowserRouter, Routes, Route, useLocation, useNavigate } from 'react-router-dom';
import { ToastProvider } from './hooks/useToast';
import Navbar from './components/Navbar';
import Dashboard from './pages/Dashboard';
import Ingest from './pages/Ingest';
import Pipeline from './pages/Pipeline';
import Clients from './pages/Clients';
import ApiSources from './pages/ApiSources';
import DataQuality from './pages/DataQuality';
import Browse from './pages/Browse';
import OrchestrationStepper from './pages/OrchestrationStepper';
import HistoryView from './pages/HistoryView';
import './index.css';
import logo from "./assets/images/image.png"
export default function App() {
  return (
    <BrowserRouter>
      <ToastProvider>
        <AppContent />
      </ToastProvider>
    </BrowserRouter>
  );
}

function AppContent() {
  const location = useLocation();
  const hideNavbar = location.pathname.startsWith('/orchestration-beta');
  const navigate = useNavigate();

  return (
    <>
      {!hideNavbar && <Navbar />}
      {hideNavbar ? (
        <Routes>
          <Route path="/orchestration-beta/*" element={<OrchestrationStepper />} />
        </Routes>
      ) : (
        <div style={{ flex: 1, maxWidth: 1200, width: '100%', margin: '0 auto', padding: '0px 28px 22px' }}>
          <Routes>
            <Route path="/"        element={<Dashboard />} />
            <Route path="/ingest"  element={<Ingest />} />
            <Route path="/pipeline"element={<Pipeline />} />
            <Route path="/clients" element={<Clients />} />
            <Route path="/apis"    element={<ApiSources />} />
            <Route path="/dq"      element={<DataQuality />} />
            <Route path="/browse"  element={<Browse />} />
            <Route path="/history" element={<HistoryView />} />
          </Routes>
        </div>
      )}
    </>
  );
}
