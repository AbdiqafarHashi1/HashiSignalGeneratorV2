import axios from "axios";

const baseURL =
  (process.env.NEXT_PUBLIC_API_BASE_URL as string | undefined) ??
  "http://localhost:8000";

const client = axios.create({
  baseURL,
  timeout: 5000,
});

export const fetchOverview = async () => {
  const { data } = await client.get("/overview");
  return data;
};

export const fetchTrades = async (limit = 50, offset = 0) => {
  const { data } = await client.get("/trades", {
    params: { limit, offset },
  });
  return data;
};

export const fetchPositions = async () => {
  const { data } = await client.get("/positions");
  return data;
};

export const fetchEngineStatus = async () => {
  const { data } = await client.get("/engine/status");
  return data;
};