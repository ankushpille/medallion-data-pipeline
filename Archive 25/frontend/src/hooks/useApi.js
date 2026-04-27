import { useState, useCallback } from 'react';

const BASE_KEY = 'dea_base_url';
export const API_BASE_URL = (process.env.REACT_APP_API_BASE_URL || 'http://127.0.0.1:8001').replace(/\/$/, '');

export function getBase() {
  return (localStorage.getItem(BASE_KEY) || API_BASE_URL).replace(/\/$/, '');
}

export function setBase(url) {
  localStorage.setItem(BASE_KEY, url.replace(/\/$/, ''));
}

export function apiUrl(path) {
  return `${getBase()}${path.startsWith('/') ? path : `/${path}`}`;
}

export async function apiCall(path, method = 'GET', body = null) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json', accept: 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(apiUrl(path), opts);
  return res.json();
}

export function useApi() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const call = useCallback(async (path, method = 'GET', body = null) => {
    setLoading(true);
    setError(null);
    try {
      const data = await apiCall(path, method, body);
      return data;
    } catch (e) {
      setError(e.message);
      throw e;
    } finally {
      setLoading(false);
    }
  }, []);

  return { call, loading, error };
}
