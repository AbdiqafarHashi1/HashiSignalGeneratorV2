import axios from 'axios';

const client = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_BASE_URL || 'http://api:8000',
  timeout: 2000,
});

export const fetchOverview = async () => (await client.get('/overview')).data;
export const fetchTrades = async (limit = 50, offset = 0) =>
  (await client.get('/trades', { params: { limit, offset } })).data;
export const fetchPositions = async () => (await client.get('/positions')).data;
export const fetchEngineStatus = async () => (await client.get('/engine/status')).data;
