import { useEffect, useState, useRef } from 'react'
import { X, Loader2, Table, Code, BarChart3, Upload, GitBranch, FileSpreadsheet, ArrowRight, ChevronRight, Pencil, Check, Download } from 'lucide-react'
import { api } from '../lib/api'
import type { Node, ColumnProfile } from '../types'

interface NodeDetailPanelProps {
  sessionId: string
  node: Node
  parentNode?: Node | null
  onClose: () => void
  onDataRefresh?: () => void
  onBranchFromNode?: (nodeId: string) => void
  onNodeRenamed?: (nodeId: string, newName: string) => void
}

type Tab = 'preview' | 'code' | 'profile'

interface NodeData {
  data: Record<string, unknown>[]
  row_count: number
  column_count: number
}

interface NodeDetail {
  transformation_code?: string
  created_at?: string
}

interface DataIssue {
  issue_type?: string
  severity?: string
  column?: string
  description: string
  affected_count?: number
  affected_percent?: number
  suggestion?: string
}

interface NodeProfile {
  row_count: number
  column_count: number
  columns: ColumnProfile[]
  issues?: DataIssue[]
}

export default function NodeDetailPanel({ sessionId, node, parentNode, onClose, onDataRefresh, onBranchFromNode, onNodeRenamed }: NodeDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>('preview')
  const [nodeData, setNodeData] = useState<NodeData | null>(null)
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null)
  const [nodeProfile, setNodeProfile] = useState<NodeProfile | null>(null)
  const [loading, setLoading] = useState(true)
  const [profileLoading, setProfileLoading] = useState(false)
  const [profileError, setProfileError] = useState('')
  const [error, setError] = useState('')
  const [replacing, setReplacing] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Node rename state
  const [isEditingName, setIsEditingName] = useState(false)
  const [editedName, setEditedName] = useState(node.transformation || '')
  const [savingName, setSavingName] = useState(false)
  const nameInputRef = useRef<HTMLInputElement>(null)

  // Download state
  const [downloading, setDownloading] = useState(false)

  const isOriginal = !node.parent_id

  // Calculate row change from parent
  const rowChange = parentNode ? node.row_count - parentNode.row_count : null
  const rowChangePercent = parentNode && parentNode.row_count > 0
    ? ((node.row_count - parentNode.row_count) / parentNode.row_count * 100).toFixed(1)
    : null

  useEffect(() => {
    loadNodeData()
    loadNodeDetail()
    if (isOriginal) {
      loadNodeProfile()
    }
  }, [node.id])

  const loadNodeData = async () => {
    setLoading(true)
    setError('')

    try {
      const data = await api.getNodeData(sessionId, node.id, 50)
      setNodeData(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  const loadNodeDetail = async () => {
    try {
      const detail = await api.getNodeDetail(sessionId, node.id)
      setNodeDetail(detail)
    } catch (err) {
      console.error('Failed to load node detail:', err)
    }
  }

  const loadNodeProfile = async () => {
    setProfileLoading(true)
    setProfileError('')
    try {
      const response = await api.getNodeProfile(sessionId, node.id)
      console.log('Profile API response:', response)
      // API returns { row_count, column_count, profile: { row_count, column_count, columns, issues } }
      // The profile field contains the full DataProfile object
      const profile = response?.profile || {}
      const columns = Array.isArray(profile.columns) ? profile.columns : []
      setNodeProfile({
        row_count: profile.row_count || response?.row_count || 0,
        column_count: profile.column_count || response?.column_count || 0,
        columns: columns,
        issues: Array.isArray(profile.issues) ? profile.issues as DataIssue[] : [],
      })
      if (columns.length === 0) {
        setProfileError('Profile data has no columns')
      }
    } catch (err) {
      console.error('Failed to load node profile:', err)
      setProfileError(err instanceof Error ? err.message : 'Failed to load profile')
    } finally {
      setProfileLoading(false)
    }
  }

  const handleReplaceFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return

    setReplacing(true)
    setError('')

    try {
      await api.uploadFile(sessionId, file)
      if (onDataRefresh) {
        onDataRefresh()
      }
      await loadNodeData()
      if (isOriginal) {
        await loadNodeProfile()
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to replace file')
    } finally {
      setReplacing(false)
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
    }
  }

  const formatTimeAgo = (dateString: string) => {
    const date = new Date(dateString)
    const now = new Date()
    const diffMs = now.getTime() - date.getTime()
    const diffMins = Math.floor(diffMs / 60000)
    const diffHours = Math.floor(diffMs / 3600000)
    const diffDays = Math.floor(diffMs / 86400000)

    if (diffMins < 1) return 'Just now'
    if (diffMins < 60) return `${diffMins} min ago`
    if (diffHours < 24) return `${diffHours} hours ago`
    if (diffDays < 7) return `${diffDays} days ago`
    return date.toLocaleDateString()
  }

  // Rename handlers
  const startEditing = () => {
    setEditedName(node.transformation || '')
    setIsEditingName(true)
    setTimeout(() => nameInputRef.current?.focus(), 0)
  }

  const cancelEditing = () => {
    setIsEditingName(false)
    setEditedName(node.transformation || '')
  }

  const saveNodeName = async () => {
    const trimmedName = editedName.trim()
    if (!trimmedName || trimmedName === node.transformation) {
      cancelEditing()
      return
    }

    setSavingName(true)
    try {
      await api.renameNode(sessionId, node.id, trimmedName)
      setIsEditingName(false)
      if (onNodeRenamed) {
        onNodeRenamed(node.id, trimmedName)
      }
      if (onDataRefresh) {
        onDataRefresh()
      }
    } catch (err) {
      console.error('Failed to rename node:', err)
      setError(err instanceof Error ? err.message : 'Failed to rename')
    } finally {
      setSavingName(false)
    }
  }

  const handleNameKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      saveNodeName()
    } else if (e.key === 'Escape') {
      cancelEditing()
    }
  }

  // Download handler
  const handleDownload = async () => {
    setDownloading(true)
    setError('')
    try {
      const filename = node.transformation
        ? `${node.transformation.slice(0, 30).replace(/[^a-z0-9]/gi, '_')}.csv`
        : `data_${node.id.slice(0, 8)}.csv`
      await api.downloadNodeData(sessionId, node.id, filename)
    } catch (err) {
      console.error('Download failed:', err)
      setError(err instanceof Error ? err.message : 'Download failed')
    } finally {
      setDownloading(false)
    }
  }

  // Close on escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', handleEscape)
    return () => window.removeEventListener('keydown', handleEscape)
  }, [onClose])

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-white rounded-xl shadow-2xl w-[1050px] max-w-[95vw] h-[650px] max-h-[90vh] flex overflow-hidden">
        {/* Left Info Panel */}
        <div className="w-[240px] flex-shrink-0 bg-gray-50 border-r border-gray-200 flex flex-col overflow-hidden">
          {/* Node Type Badge */}
          <div className="px-4 py-4 border-b border-gray-200">
            <div className="flex items-center gap-2 mb-2">
              <div className={`p-1.5 rounded-lg ${isOriginal ? 'bg-blue-100' : 'bg-emerald-100'}`}>
                {isOriginal ? (
                  <FileSpreadsheet className={`w-4 h-4 ${isOriginal ? 'text-blue-600' : 'text-emerald-600'}`} />
                ) : (
                  <ArrowRight className="w-4 h-4 text-emerald-600" />
                )}
              </div>
              <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
                isOriginal ? 'bg-blue-100 text-blue-700' : 'bg-emerald-100 text-emerald-700'
              }`}>
                {isOriginal ? 'Original Data' : 'Transformation'}
              </span>
            </div>
            {/* Editable node name */}
            {isEditingName ? (
              <div className="flex items-center gap-1.5">
                <input
                  ref={nameInputRef}
                  type="text"
                  value={editedName}
                  onChange={(e) => setEditedName(e.target.value)}
                  onKeyDown={handleNameKeyDown}
                  onBlur={saveNodeName}
                  disabled={savingName}
                  className="flex-1 px-2 py-1 text-sm font-semibold text-gray-900 bg-white border border-blue-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-400"
                  placeholder="Enter name..."
                />
                <button
                  onClick={saveNodeName}
                  disabled={savingName}
                  className="p-1 text-blue-600 hover:bg-blue-50 rounded transition-colors"
                >
                  {savingName ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Check className="w-4 h-4" />
                  )}
                </button>
              </div>
            ) : (
              <div className="group flex items-start gap-1.5">
                <h3 className="flex-1 font-semibold text-gray-900 text-sm leading-tight">
                  {node.transformation || 'Source File'}
                </h3>
                {!isOriginal && (
                  <button
                    onClick={startEditing}
                    className="p-0.5 text-gray-400 hover:text-gray-600 opacity-0 group-hover:opacity-100 transition-opacity"
                    title="Rename this step"
                  >
                    <Pencil className="w-3.5 h-3.5" />
                  </button>
                )}
              </div>
            )}
            <p className="text-xs text-gray-500 mt-1">
              {formatTimeAgo(node.created_at)}
            </p>
          </div>

          {/* Stats */}
          <div className="px-4 py-3 border-b border-gray-200">
            <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Output</h4>
            <div className="space-y-1.5">
              <div className="flex justify-between text-sm">
                <span className="text-gray-600">Rows</span>
                <span className="font-medium text-gray-900">{node.row_count.toLocaleString()}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-600">Columns</span>
                <span className="font-medium text-gray-900">{node.column_count}</span>
              </div>
              {rowChange !== null && (
                <div className="flex justify-between text-sm">
                  <span className="text-gray-600">Row Change</span>
                  <span className={`font-medium ${rowChange >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                    {rowChange >= 0 ? '+' : ''}{rowChange.toLocaleString()} ({rowChangePercent}%)
                  </span>
                </div>
              )}
            </div>
          </div>

          {/* Input Reference (if has parent) */}
          {parentNode && (
            <div className="px-4 py-3 border-b border-gray-200">
              <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">Input</h4>
              <div className="p-2 bg-white rounded-lg border border-gray-200">
                <div className="flex items-center gap-2">
                  <ChevronRight className="w-3.5 h-3.5 text-gray-400" />
                  <div className="min-w-0">
                    <p className="text-xs font-medium text-gray-700 truncate">
                      {parentNode.transformation || 'Original Data'}
                    </p>
                    <p className="text-xs text-gray-500">
                      {parentNode.row_count.toLocaleString()} rows
                    </p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Spacer */}
          <div className="flex-1" />

          {/* Actions */}
          <div className="px-4 py-3 border-t border-gray-200 space-y-2">
            {/* Download - available for all nodes */}
            <button
              onClick={handleDownload}
              disabled={downloading}
              className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-emerald-600 bg-emerald-50 hover:bg-emerald-100 rounded-lg transition-colors disabled:opacity-50"
            >
              {downloading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Download className="w-4 h-4" />
              )}
              {downloading ? 'Downloading...' : 'Download CSV'}
            </button>

            {onBranchFromNode && !isOriginal && (
              <button
                onClick={() => onBranchFromNode(node.id)}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-blue-600 bg-blue-50 hover:bg-blue-100 rounded-lg transition-colors"
              >
                <GitBranch className="w-4 h-4" />
                Branch from here
              </button>
            )}
            {isOriginal && (
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={replacing}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-gray-600 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg transition-colors disabled:opacity-50"
              >
                {replacing ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Upload className="w-4 h-4" />
                )}
                Replace File
              </button>
            )}
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleReplaceFile}
              className="hidden"
            />
          </div>
        </div>

        {/* Right Content Area */}
        <div className="flex-1 flex flex-col overflow-hidden">
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-3 border-b border-gray-200">
            <div className="flex items-center gap-4">
              {/* Tabs */}
              <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
                <button
                  onClick={() => setActiveTab('preview')}
                  className={`flex items-center gap-2 px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                    activeTab === 'preview'
                      ? 'bg-white text-gray-900 shadow-sm'
                      : 'text-gray-600 hover:text-gray-900'
                  }`}
                >
                  <Table className="w-4 h-4" />
                  Preview
                </button>
                {isOriginal && (
                  <button
                    onClick={() => setActiveTab('profile')}
                    className={`flex items-center gap-2 px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                      activeTab === 'profile'
                        ? 'bg-white text-gray-900 shadow-sm'
                        : 'text-gray-600 hover:text-gray-900'
                    }`}
                  >
                    <BarChart3 className="w-4 h-4" />
                    Profile
                  </button>
                )}
                {nodeDetail?.transformation_code && (
                  <button
                    onClick={() => setActiveTab('code')}
                    className={`flex items-center gap-2 px-3 py-1.5 text-sm font-medium rounded-md transition-colors ${
                      activeTab === 'code'
                        ? 'bg-white text-gray-900 shadow-sm'
                        : 'text-gray-600 hover:text-gray-900'
                    }`}
                  >
                    <Code className="w-4 h-4" />
                    Code
                  </button>
                )}
              </div>
            </div>

            <button
              onClick={onClose}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Content */}
          <div className="flex-1 overflow-hidden">
          {error && (
            <div className="m-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm">
              {error}
            </div>
          )}

          {activeTab === 'preview' && (
            loading ? (
              <div className="flex items-center justify-center h-full">
                <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
              </div>
            ) : (
              <div className="h-full flex flex-col">
                {/* Data Table */}
                <div className="flex-1 overflow-auto">
                  {nodeData && nodeData.data.length > 0 ? (
                    <table className="min-w-full text-sm">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          {Object.keys(nodeData.data[0]).map((col) => (
                            <th
                              key={col}
                              className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase tracking-wider border-b border-gray-200"
                            >
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100 bg-white">
                        {nodeData.data.map((row, rowIdx) => (
                          <tr key={rowIdx} className="hover:bg-gray-50">
                            {Object.keys(nodeData.data[0]).map((col) => (
                              <td
                                key={col}
                                className="px-4 py-2.5 text-gray-700 whitespace-nowrap max-w-[250px] truncate"
                              >
                                {String(row[col] ?? '')}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : (
                    <div className="flex items-center justify-center h-full text-gray-400">
                      No data available
                    </div>
                  )}
                </div>

                {/* Row Count Footer */}
                {nodeData && (
                  <div className="px-4 py-2.5 bg-gray-50 border-t border-gray-200 text-xs text-gray-500">
                    Showing {nodeData.data.length} of {nodeData.row_count.toLocaleString()} rows
                  </div>
                )}
              </div>
            )
          )}

          {activeTab === 'profile' && (
            profileLoading ? (
              <div className="flex items-center justify-center h-full">
                <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
              </div>
            ) : nodeProfile && nodeProfile.columns.length > 0 ? (
              <div className="h-full overflow-auto p-6">
                {/* Summary Cards */}
                <div className="grid grid-cols-3 gap-4 mb-6">
                  <div className="bg-blue-50 rounded-lg p-4">
                    <div className="text-2xl font-bold text-blue-700">
                      {nodeProfile.row_count.toLocaleString()}
                    </div>
                    <div className="text-sm text-blue-600">Total Rows</div>
                  </div>
                  <div className="bg-emerald-50 rounded-lg p-4">
                    <div className="text-2xl font-bold text-emerald-700">
                      {nodeProfile.column_count}
                    </div>
                    <div className="text-sm text-emerald-600">Columns</div>
                  </div>
                  <div className="bg-amber-50 rounded-lg p-4">
                    <div className="text-2xl font-bold text-amber-700">
                      {nodeProfile.columns.filter(c => c.null_count > 0).length}
                    </div>
                    <div className="text-sm text-amber-600">Columns with Nulls</div>
                  </div>
                </div>

                {/* Column Details Table */}
                <h3 className="text-sm font-semibold text-gray-900 mb-3">Column Details</h3>
                <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
                  <table className="min-w-full text-sm">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Column</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Type</th>
                        <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Semantic</th>
                        <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Unique</th>
                        <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Nulls</th>
                        <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Null %</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {nodeProfile.columns.map((col) => (
                        <tr key={col.name} className="hover:bg-gray-50">
                          <td className="px-4 py-3 font-medium text-gray-900">{col.name}</td>
                          <td className="px-4 py-3 text-gray-600">
                            <span className="px-2 py-0.5 bg-gray-100 rounded text-xs font-mono">
                              {col.dtype}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-gray-600">{col.semantic_type}</td>
                          <td className="px-4 py-3 text-right text-gray-600">{col.unique_count.toLocaleString()}</td>
                          <td className="px-4 py-3 text-right">
                            <span className={col.null_count > 0 ? 'text-amber-600 font-medium' : 'text-gray-600'}>
                              {col.null_count.toLocaleString()}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right">
                            {col.null_count > 0 ? (
                              <div className="flex items-center justify-end gap-2">
                                <div className="w-16 bg-gray-200 rounded-full h-1.5">
                                  <div
                                    className="bg-amber-500 h-1.5 rounded-full"
                                    style={{ width: `${Math.min(col.null_percent, 100)}%` }}
                                  />
                                </div>
                                <span className="text-amber-600 font-medium w-12 text-right">
                                  {col.null_percent.toFixed(1)}%
                                </span>
                              </div>
                            ) : (
                              <span className="text-gray-400">0%</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Issues */}
                {nodeProfile.issues && nodeProfile.issues.length > 0 && (
                  <div className="mt-6">
                    <h3 className="text-sm font-semibold text-gray-900 mb-3">Detected Issues</h3>
                    <div className="space-y-2">
                      {nodeProfile.issues.map((issue, idx) => (
                        <div key={idx} className="p-3 bg-amber-50 rounded-lg border border-amber-200">
                          <div className="flex items-start gap-2">
                            <span className={`text-xs font-medium px-2 py-0.5 rounded ${
                              issue.severity === 'critical' ? 'bg-red-100 text-red-700' :
                              issue.severity === 'warning' ? 'bg-amber-100 text-amber-700' :
                              'bg-blue-100 text-blue-700'
                            }`}>
                              {issue.severity || 'info'}
                            </span>
                            {issue.column && (
                              <span className="text-xs text-gray-500">[{issue.column}]</span>
                            )}
                          </div>
                          <p className="text-sm text-gray-700 mt-1">{issue.description}</p>
                          {issue.suggestion && (
                            <p className="text-xs text-gray-500 mt-1">Suggestion: {issue.suggestion}</p>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex flex-col items-center justify-center h-full text-gray-400 p-6">
                <p className="text-gray-600 font-medium">No profile data available</p>
                {profileError && (
                  <p className="text-red-500 text-sm mt-2">{profileError}</p>
                )}
                <p className="text-xs mt-2 text-gray-400">
                  {nodeProfile ? `Columns found: ${nodeProfile.columns.length}` : 'Profile not loaded'}
                </p>
                <p className="text-xs mt-1 text-gray-400">
                  Node ID: {node.id.slice(0, 8)}... | Original: {isOriginal ? 'Yes' : 'No'}
                </p>
              </div>
            )
          )}

          {activeTab === 'code' && (
            <div className="h-full overflow-auto p-6">
              <div className="bg-gray-900 rounded-lg p-4 overflow-x-auto">
                <pre className="text-sm text-gray-100 font-mono whitespace-pre-wrap">
                  {nodeDetail?.transformation_code || 'No code available'}
                </pre>
              </div>
            </div>
          )}
          </div>
        </div>
      </div>
    </div>
  )
}
