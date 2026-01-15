import { useEffect, useState } from 'react'
import { X, Loader2, Table, Code, FileSpreadsheet } from 'lucide-react'
import { api } from '../lib/api'
import type { Node } from '../types'

interface NodeDetailPanelProps {
  sessionId: string
  node: Node
  onClose: () => void
}

type Tab = 'preview' | 'code'

interface NodeData {
  rows: Record<string, unknown>[]
  columns: string[]
  total_rows: number
}

export default function NodeDetailPanel({ sessionId, node, onClose }: NodeDetailPanelProps) {
  const [activeTab, setActiveTab] = useState<Tab>('preview')
  const [nodeData, setNodeData] = useState<NodeData | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    loadNodeData()
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

  const isOriginal = !node.parent_id

  return (
    <div className="w-[500px] bg-white border-l border-gray-200 flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
        <div className="flex items-center gap-3">
          <div
            className={`w-8 h-8 rounded-lg flex items-center justify-center ${
              isOriginal ? 'bg-emerald-100' : 'bg-blue-100'
            }`}
          >
            <FileSpreadsheet
              className={`w-4 h-4 ${isOriginal ? 'text-emerald-600' : 'text-blue-600'}`}
            />
          </div>
          <div>
            <h3 className="font-medium text-gray-900 text-sm">
              {node.transformation || 'Original Data'}
            </h3>
            <p className="text-xs text-gray-500">
              {node.row_count.toLocaleString()} rows x {node.column_count} columns
            </p>
          </div>
        </div>
        <button
          onClick={onClose}
          className="p-1.5 hover:bg-gray-100 rounded-lg transition-colors"
        >
          <X className="w-4 h-4 text-gray-500" />
        </button>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-gray-200">
        <button
          onClick={() => setActiveTab('preview')}
          className={`flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === 'preview'
              ? 'border-blue-600 text-blue-600'
              : 'border-transparent text-gray-500 hover:text-gray-700'
          }`}
        >
          <Table className="w-4 h-4" />
          Preview
        </button>
        {node.transformation_code && (
          <button
            onClick={() => setActiveTab('code')}
            className={`flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === 'code'
                ? 'border-blue-600 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            <Code className="w-4 h-4" />
            Code
          </button>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center h-full">
            <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
          </div>
        ) : error ? (
          <div className="p-4 text-red-600 text-sm">{error}</div>
        ) : activeTab === 'preview' ? (
          <div className="h-full flex flex-col">
            {/* Transformation Description */}
            {node.transformation && (
              <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
                <p className="text-sm text-gray-600">
                  <span className="font-medium text-gray-900">Change: </span>
                  {node.transformation}
                </p>
              </div>
            )}

            {/* Data Table */}
            <div className="flex-1 overflow-auto">
              {nodeData && nodeData.rows.length > 0 ? (
                <table className="min-w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      {nodeData.columns.map((col) => (
                        <th
                          key={col}
                          className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider border-b border-gray-200"
                        >
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {nodeData.rows.map((row, rowIdx) => (
                      <tr key={rowIdx} className="hover:bg-gray-50">
                        {nodeData.columns.map((col) => (
                          <td
                            key={col}
                            className="px-3 py-2 text-gray-700 whitespace-nowrap max-w-[200px] truncate"
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
              <div className="px-4 py-2 bg-gray-50 border-t border-gray-200 text-xs text-gray-500">
                Showing {nodeData.rows.length} of {nodeData.total_rows.toLocaleString()} rows
              </div>
            )}
          </div>
        ) : (
          // Code Tab
          <div className="h-full overflow-auto p-4">
            <pre className="text-xs text-gray-700 font-mono bg-gray-50 p-4 rounded-lg overflow-x-auto">
              {node.transformation_code || 'No code available'}
            </pre>
          </div>
        )}
      </div>
    </div>
  )
}
