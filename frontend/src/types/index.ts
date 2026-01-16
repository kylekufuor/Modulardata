// User types
export interface User {
  id: string
  email: string
  first_name?: string
  last_name?: string
}

// Session types
export interface Session {
  session_id: string
  status: 'draft' | 'deployed' | 'archived'
  created_at: string
  original_filename?: string
  current_node_id?: string
  deployed_node_id?: string
  deployed_at?: string
}

// Node types
export interface Node {
  id: string
  parent_id: string | null
  created_at: string
  transformation: string | null
  transformation_code?: string
  step_descriptions?: string[]
  row_count: number
  column_count: number
  is_current: boolean
  storage_path?: string
  preview_rows?: Record<string, unknown>[]
}

// Chat types
export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  node_id?: string
  created_at: string
}

// Plan types
export interface PlanStep {
  step_number: number
  transformation_type: string
  explanation: string
  target_columns: string[]
}

export interface Plan {
  id: string
  status: string
  step_count: number
  steps: PlanStep[]
}

// API Response types
export interface SessionListResponse {
  sessions: Session[]
  total: number
  page: number
  page_size: number
}

export interface HistoryResponse {
  session_id: string
  current_node_id: string | null
  total_nodes: number
  total_messages: number
  nodes: Node[]
  messages: ChatMessage[]
}

export interface ChatResponse {
  session_id: string
  message: string
  plan: Plan
  assistant_response: string
}

export interface UploadResponse {
  session_id: string
  node_id: string
  filename: string
  storage_path: string
  profile: {
    row_count: number
    column_count: number
    columns: ColumnProfile[]
    issues: string[]
  }
  preview: Record<string, unknown>[]
}

export interface ColumnProfile {
  name: string
  dtype: string
  semantic_type: string
  null_count: number
  null_percent: number
  unique_count: number
}

// Risk assessment types
export interface RiskPreview {
  rows_before: number
  cols_before: number
  rows_after?: number
  rows_removed?: number
  removal_percent?: number
  columns_removed?: string[]
  sample_removed?: Record<string, unknown>[]
}

export interface ApplyPlanResponse {
  success: boolean
  node_id?: string
  transformations_applied?: number
  rows_before?: number
  rows_after?: number
  message: string
  error?: string
  // Risk assessment fields
  requires_confirmation?: boolean
  is_risky?: boolean
  risk_level?: 'none' | 'moderate' | 'high'
  risk_reasons?: string[]
  risk_preview?: RiskPreview
  confirmation_message?: string
}
