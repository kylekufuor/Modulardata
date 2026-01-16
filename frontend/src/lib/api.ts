import { supabase } from './supabase'

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

async function getAuthHeaders(): Promise<HeadersInit> {
  const { data: { session } } = await supabase.auth.getSession()

  if (!session?.access_token) {
    throw new Error('Not authenticated')
  }

  return {
    'Authorization': `Bearer ${session.access_token}`,
    'Content-Type': 'application/json',
  }
}

async function fetchWithAuth(endpoint: string, options: RequestInit = {}) {
  const headers = await getAuthHeaders()

  const response = await fetch(`${API_URL}${endpoint}`, {
    ...options,
    headers: {
      ...headers,
      ...options.headers,
    },
  })

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Request failed' }))
    throw new Error(error.detail || 'Request failed')
  }

  return response.json()
}

// Sessions API
export const api = {
  // Sessions
  listSessions: () => fetchWithAuth('/api/v1/sessions'),

  getSession: (sessionId: string) => fetchWithAuth(`/api/v1/sessions/${sessionId}`),

  createSession: () => fetchWithAuth('/api/v1/sessions', { method: 'POST' }),

  deleteSession: (sessionId: string) => fetchWithAuth(`/api/v1/sessions/${sessionId}`, { method: 'DELETE' }),

  renameModule: (sessionId: string, name: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}`, {
      method: 'PATCH',
      body: JSON.stringify({ name }),
    }),

  // Upload
  uploadFile: async (sessionId: string, file: File) => {
    const { data: { session } } = await supabase.auth.getSession()
    if (!session?.access_token) throw new Error('Not authenticated')

    const formData = new FormData()
    formData.append('file', file)

    const response = await fetch(`${API_URL}/api/v1/sessions/${sessionId}/upload`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${session.access_token}`,
      },
      body: formData,
    })

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Upload failed' }))
      throw new Error(error.detail || 'Upload failed')
    }

    return response.json()
  },

  // Chat
  sendMessage: (sessionId: string, message: string, mode: 'plan' | 'transform' = 'plan') =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/chat`, {
      method: 'POST',
      body: JSON.stringify({ message, mode }),
    }),

  // Plan
  getPlan: (sessionId: string) => fetchWithAuth(`/api/v1/sessions/${sessionId}/plan`),

  applyPlan: (sessionId: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/plan/apply`, { method: 'POST' }),

  clearPlan: (sessionId: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/plan/clear`, { method: 'POST' }),

  // History / Nodes
  getHistory: (sessionId: string) => fetchWithAuth(`/api/v1/sessions/${sessionId}/history`),

  getNodes: (sessionId: string) => fetchWithAuth(`/api/v1/sessions/${sessionId}/nodes`),

  getNodeDetail: (sessionId: string, nodeId: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/nodes/${nodeId}`),

  renameNode: (sessionId: string, nodeId: string, name: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/nodes/${nodeId}`, {
      method: 'PATCH',
      body: JSON.stringify({ name }),
    }),

  // Data
  getPreview: (sessionId: string, rows: number = 10) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/preview?rows=${rows}`),

  getNodeData: (sessionId: string, nodeId: string, limit: number = 100) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/nodes/${nodeId}/data?format=json&limit=${limit}`),

  getNodeProfile: (sessionId: string, nodeId: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/nodes/${nodeId}/profile`),
}
