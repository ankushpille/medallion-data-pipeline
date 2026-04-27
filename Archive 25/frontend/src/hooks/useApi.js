import { useState, useCallback } from 'react';

const BASE_KEY = 'dea_base_url';

export function getBase() {
  return localStorage.getItem(BASE_KEY) || '';
}

export function setBase(url) {
  localStorage.setItem(BASE_KEY, url.replace(/\/$/, ''));
}

export async function apiCall(path, method = 'GET', body = null) {
  const base = getBase();
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json', accept: 'application/json' },
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(base + path, opts);
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
