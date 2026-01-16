import { useEffect, useState, useRef } from 'react'
import { X, Loader2, Table, Code, BarChart3, Upload, Clock } from 'lucide-react'
import { api } from '../lib/api'
import type { Node, ColumnProfile } from '../types'

interface NodeDetailPanelProps {
  sessionId: string
  node: Node
  onClose: () => void
  onDataRefresh?: () => void
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

interface NodeProfile {
  row_count: number
  column_count: number
  columns: ColumnProfile[]
  issues?: string[]
}

export default function NodeDetailPanel({ sessionId, node, onClose, onDataRefresh }: NodeDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>('preview')
  const [nodeData, setNodeData] = useState<NodeData | null>(null)
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null)
  const [nodeProfile, setNodeProfile] = useState<NodeProfile | null>(null)
  const [loading, setLoading] = useState(true)
  const [profileLoading, setProfileLoading] = useState(false)
  const [error, setError] = useState('')
  const [replacing, setReplacing] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const isOriginal = !node.parent_id

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
    try {
      const response = await api.getNodeProfile(sessionId, node.id)
      // API returns { row_count, column_count, profile: { columns, issues } }
      setNodeProfile({
        row_count: response.row_count,
        column_count: response.column_count,
        columns: response.profile?.columns || [],
        issues: response.profile?.issues || [],
      })
    } catch (err) {
      console.error('Failed to load node profile:', err)
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
      <div className="relative bg-white rounded-xl shadow-2xl w-[900px] max-w-[90vw] h-[600px] max-h-[85vh] flex flex-col overflow-hidden">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
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

          <div className="flex items-center gap-3">
            {isOriginal && (
              <>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  disabled={replacing}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors disabled:opacity-50"
                >
                  {replacing ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Upload className="w-4 h-4" />
                  )}
                  Replace
                </button>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  onChange={handleReplaceFile}
                  className="hidden"
                />
              </>
            )}
            <button
              onClick={onClose}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
            >
              Close
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Metadata Bar */}
        <div className="flex items-center gap-6 px-6 py-3 bg-gray-50 border-b border-gray-200 text-sm text-gray-600">
          <span>{node.row_count.toLocaleString()} rows</span>
          <span className="text-gray-300">|</span>
          <span>{node.column_count} columns</span>
          <span className="text-gray-300">|</span>
          <span className="flex items-center gap-1.5">
            <Clock className="w-3.5 h-3.5" />
            {formatTimeAgo(node.created_at)}
          </span>
          {node.transformation && (
            <>
              <span className="text-gray-300">|</span>
              <span className="text-gray-900 font-medium truncate max-w-md" title={node.transformation}>
                {node.transformation}
              </span>
            </>
          )}
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
            ) : nodeProfile ? (
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
                        <div key={idx} className="flex items-start gap-2 p-3 bg-amber-50 rounded-lg">
                          <span className="text-amber-600 text-sm">{issue}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            ) : (
              <div className="flex items-center justify-center h-full text-gray-400">
                No profile data available
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
  )
}
