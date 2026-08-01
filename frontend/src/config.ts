export const API_URI = (import.meta.env.VITE_API_URI || 'http://localhost:8000').replace(/\/+$/, '');

export const getWsUrl = (): string => {
  return `${API_URI.replace(/^http/, 'ws')}/ws`;
};
