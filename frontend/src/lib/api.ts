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

  // Download
  downloadNodeData: async (sessionId: string, nodeId: string, filename?: string) => {
    const { data: { session } } = await supabase.auth.getSession()
    if (!session?.access_token) throw new Error('Not authenticated')

    const response = await fetch(
      `${API_URL}/api/v1/sessions/${sessionId}/nodes/${nodeId}/data?format=csv`,
      {
        headers: {
          'Authorization': `Bearer ${session.access_token}`,
        },
      }
    )

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Download failed' }))
      throw new Error(error.detail || 'Download failed')
    }

    // Get the blob and trigger download
    const blob = await response.blob()
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename || `data_${nodeId.slice(0, 8)}.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    window.URL.revokeObjectURL(url)
  },

  // Module Runs
  runModule: async (sessionId: string, file: File, force: boolean = false) => {
    const { data: { session } } = await supabase.auth.getSession()
    if (!session?.access_token) throw new Error('Not authenticated')

    const formData = new FormData()
    formData.append('file', file)

    const url = `${API_URL}/api/v1/sessions/${sessionId}/run${force ? '?force=true' : ''}`

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${session.access_token}`,
      },
      body: formData,
    })

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Run failed' }))
      throw new Error(error.detail || 'Run failed')
    }

    return response.json()
  },

  listRuns: (sessionId: string, limit: number = 50, offset: number = 0) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/runs?limit=${limit}&offset=${offset}`),

  getRunDetail: (sessionId: string, runId: string) =>
    fetchWithAuth(`/api/v1/sessions/${sessionId}/runs/${runId}`),

  downloadRunOutput: async (sessionId: string, runId: string, filename?: string) => {
    const { data: { session } } = await supabase.auth.getSession()
    if (!session?.access_token) throw new Error('Not authenticated')

    const response = await fetch(
      `${API_URL}/api/v1/sessions/${sessionId}/runs/${runId}/download`,
      {
        headers: {
          'Authorization': `Bearer ${session.access_token}`,
        },
      }
    )

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Download failed' }))
      throw new Error(error.detail || 'Download failed')
    }

    // Get the blob and trigger download
    const blob = await response.blob()
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = filename || `run_${runId.slice(0, 8)}_output.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    window.URL.revokeObjectURL(url)
  },

  confirmAndRunModule: async (sessionId: string, runId: string, file: File) => {
    const { data: { session } } = await supabase.auth.getSession()
    if (!session?.access_token) throw new Error('Not authenticated')

    const formData = new FormData()
    formData.append('file', file)

    const response = await fetch(
      `${API_URL}/api/v1/sessions/${sessionId}/runs/${runId}/confirm`,
      {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${session.access_token}`,
        },
        body: formData,
      }
    )

    if (!response.ok) {
      const error = await response.json().catch(() => ({ detail: 'Confirm failed' }))
      throw new Error(error.detail || 'Confirm failed')
    }

    return response.json()
  },
}
