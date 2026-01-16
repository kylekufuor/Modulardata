import { useEffect, useState, useCallback, useRef } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import {
  ReactFlow,
  Background,
  Controls,
  useNodesState,
  useEdgesState,
  Position,
  type Node as FlowNode,
  type Edge,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { ArrowLeft, Send, Upload, Loader2, Pencil, Check, X } from 'lucide-react'
import { api } from '../lib/api'
import type { Node, ChatMessage, HistoryResponse, UploadResponse, Plan } from '../types'
import DataNode from '../components/DataNode'
import NodeDetailPanel from '../components/NodeDetailPanel'

const nodeTypes = {
  dataNode: DataNode,
}

export default function SessionPage() {
  const { sessionId } = useParams<{ sessionId: string }>()
  const navigate = useNavigate()

  // State
  const [nodes, setNodes, onNodesChange] = useNodesState<FlowNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [inputMessage, setInputMessage] = useState('')
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [dataNodes, setDataNodes] = useState<Node[]>([])
  const [currentPlan, setCurrentPlan] = useState<Plan | null>(null)
  const [moduleName, setModuleName] = useState<string>('')

  // Loading states
  const [loading, setLoading] = useState(true)
  const [editingName, setEditingName] = useState(false)
  const [editName, setEditName] = useState('')
  const [savingName, setSavingName] = useState(false)
  const nameInputRef = useRef<HTMLInputElement>(null)
  const [uploading, setUploading] = useState(false)
  const [sending, setSending] = useState(false)
  const [applyingPlan, setApplyingPlan] = useState(false)
  const [error, setError] = useState('')

  const fileInputRef = useRef<HTMLInputElement>(null)
  const chatEndRef = useRef<HTMLDivElement>(null)

  // Load session data
  useEffect(() => {
    if (sessionId) {
      loadSessionData()
    }
  }, [sessionId])

  // Scroll chat to bottom when messages change
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  // Focus name input when editing starts
  useEffect(() => {
    if (editingName && nameInputRef.current) {
      nameInputRef.current.focus()
      nameInputRef.current.select()
    }
  }, [editingName])

  const loadSessionData = async (preserveMessages = false) => {
    if (!sessionId) return

    try {
      // Fetch session details for module name
      const sessionDetails = await api.getSession(sessionId)
      setModuleName(sessionDetails.original_filename || 'Untitled Module')

      const history: HistoryResponse = await api.getHistory(sessionId)
      setDataNodes(history.nodes)

      // Only update messages if not preserving (e.g., after file upload with welcome message)
      if (!preserveMessages) {
        setMessages(history.messages)
      }

      buildFlowGraph(history.nodes, history.current_node_id)

      // Check for pending plan
      try {
        const planData = await api.getPlan(sessionId)
        if (planData.plan && planData.plan.status === 'pending') {
          setCurrentPlan(planData.plan)
        }
      } catch {
        // No pending plan
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load session')
    } finally {
      setLoading(false)
    }
  }

  const buildFlowGraph = (nodes: Node[], currentNodeId: string | null) => {
    if (nodes.length === 0) return

    const flowNodes: FlowNode[] = nodes.map((node, index) => ({
      id: node.id,
      type: 'dataNode',
      position: { x: index * 220, y: 100 },
      data: {
        label: node.transformation || 'Original Data',
        rowCount: node.row_count,
        colCount: node.column_count,
        isSelected: node.id === currentNodeId,
        isCurrent: node.is_current,
      },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
    }))

    const flowEdges: Edge[] = nodes
      .filter((node) => node.parent_id)
      .map((node) => ({
        id: `e-${node.parent_id}-${node.id}`,
        source: node.parent_id!,
        target: node.id,
        animated: node.is_current,
        style: { stroke: '#3b82f6', strokeWidth: 2 },
      }))

    setNodes(flowNodes)
    setEdges(flowEdges)
  }

  const handleNodeClick = useCallback((_: unknown, node: FlowNode) => {
    setSelectedNodeId(node.id)
  }, [])

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !sessionId) return

    setUploading(true)
    setError('')

    try {
      const response: UploadResponse = await api.uploadFile(sessionId, file)

      // Build welcome message with data profiler
      const welcomeMessage = buildWelcomeMessage(response)

      const uploadMessage: ChatMessage = {
        id: `upload-${Date.now()}`,
        role: 'assistant',
        content: welcomeMessage,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, uploadMessage])

      // Reload session data to get the new node, but preserve the welcome message
      await loadSessionData(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setUploading(false)
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
    }
  }

  const buildWelcomeMessage = (response: UploadResponse): string => {
    const { filename, profile } = response
    const { row_count, column_count, columns } = profile

    let message = `Welcome to ModularData! 👋\n\n`
    message += `I'm your data transformation assistant. I help you clean, transform, and prepare your data through natural conversation.\n\n`
    message += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`
    message += `📊 Your Data: ${filename}\n`
    message += `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`
    message += `Rows: ${row_count.toLocaleString()}  |  Columns: ${column_count}\n\n`

    // List columns with types and missing values
    message += `📋 Columns:\n`
    columns.forEach((col) => {
      const nullInfo = col.null_count > 0
        ? ` (${col.null_count} missing)`
        : ''
      message += `  • ${col.name} [${col.semantic_type}]${nullInfo}\n`
    })

    // Show issues if any columns have missing values
    const columnsWithIssues = columns.filter(col => col.null_count > 0)
      .sort((a, b) => b.null_count - a.null_count)

    if (columnsWithIssues.length > 0) {
      message += `\n⚠️ Issues Detected:\n`
      columnsWithIssues.slice(0, 5).forEach((col) => {
        const pct = row_count > 0 ? ((col.null_count / row_count) * 100).toFixed(1) : '0'
        message += `  • ${col.null_count.toLocaleString()} missing ${col.name} (${pct}%)\n`
      })
      message += `\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`
      message += `What would you like to tackle first?\n`
      message += `(I noticed ${columnsWithIssues[0].name} has the most missing values)`
    } else {
      message += `\n✅ No obvious issues detected - your data looks clean!\n`
      message += `\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n`
      message += `What would you like to do with this data?`
    }

    return message
  }

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!inputMessage.trim() || !sessionId || sending) return

    const userMessage = inputMessage.trim()
    setInputMessage('')
    setSending(true)

    // Add user message immediately
    const tempUserMsg: ChatMessage = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: userMessage,
      created_at: new Date().toISOString(),
    }
    setMessages((prev) => [...prev, tempUserMsg])

    try {
      const response = await api.sendMessage(sessionId, userMessage)

      // Add assistant response
      const assistantMsg: ChatMessage = {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        content: response.assistant_response,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, assistantMsg])

      // Set plan if returned
      if (response.plan) {
        setCurrentPlan(response.plan)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to send message')
    } finally {
      setSending(false)
    }
  }

  const handleApplyPlan = async () => {
    if (!sessionId || !currentPlan) return

    setApplyingPlan(true)
    try {
      await api.applyPlan(sessionId)
      setCurrentPlan(null)
      await loadSessionData()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to apply plan')
    } finally {
      setApplyingPlan(false)
    }
  }

  const handleClearPlan = async () => {
    if (!sessionId) return

    try {
      await api.clearPlan(sessionId)
      setCurrentPlan(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to clear plan')
    }
  }

  const startEditingName = () => {
    setEditName(moduleName)
    setEditingName(true)
  }

  const cancelEditingName = () => {
    setEditingName(false)
    setEditName('')
  }

  const saveModuleName = async () => {
    if (!sessionId || !editName.trim()) return

    setSavingName(true)
    try {
      await api.renameModule(sessionId, editName.trim())
      setModuleName(editName.trim())
      setEditingName(false)
      setEditName('')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to rename module')
    } finally {
      setSavingName(false)
    }
  }

  const handleNameKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      saveModuleName()
    } else if (e.key === 'Escape') {
      cancelEditingName()
    }
  }

  const selectedDataNode = dataNodes.find((n) => n.id === selectedNodeId)

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <Loader2 className="w-8 h-8 animate-spin text-gray-400" />
      </div>
    )
  }

  return (
    <div className="h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 px-4 py-3 flex items-center gap-4">
        <button
          onClick={() => navigate('/dashboard')}
          className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
        >
          <ArrowLeft className="w-5 h-5 text-gray-600" />
        </button>
        <div className="flex items-center gap-2">
          {editingName ? (
            <div className="flex items-center gap-2">
              <input
                ref={nameInputRef}
                type="text"
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                onKeyDown={handleNameKeyDown}
                className="px-2 py-1 text-sm font-semibold border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                disabled={savingName}
              />
              <button
                onClick={saveModuleName}
                disabled={savingName || !editName.trim()}
                className="p-1 text-green-600 hover:bg-green-50 rounded disabled:opacity-50"
              >
                {savingName ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Check className="w-4 h-4" />
                )}
              </button>
              <button
                onClick={cancelEditingName}
                disabled={savingName}
                className="p-1 text-gray-500 hover:bg-gray-100 rounded disabled:opacity-50"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          ) : (
            <>
              <h1 className="font-semibold text-gray-900">{moduleName}</h1>
              <button
                onClick={startEditingName}
                className="p-1 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded transition-colors"
                title="Rename module"
              >
                <Pencil className="w-4 h-4" />
              </button>
            </>
          )}
        </div>
        <span className="text-xs text-blue-600 font-medium bg-blue-50 px-2 py-1 rounded">CSV Transformation</span>
      </header>

      {error && (
        <div className="mx-4 mt-4 p-3 bg-red-50 text-red-700 rounded-lg text-sm">
          {error}
        </div>
      )}

      {/* Main Content - Split Pane */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Panel - Chat */}
        <div className="w-96 flex flex-col bg-white border-r border-gray-200">
          {/* Chat Messages */}
          <div className="flex-1 overflow-y-auto p-4 space-y-4">
            {dataNodes.length === 0 ? (
              /* Upload prompt - shown before any data is uploaded */
              <div className="flex flex-col items-center justify-center h-full text-center">
                <div className="w-16 h-16 bg-blue-100 rounded-full flex items-center justify-center mb-4">
                  <Upload className="w-8 h-8 text-blue-600" />
                </div>
                <h3 className="text-lg font-medium text-gray-900 mb-2">
                  Upload a CSV file to get started
                </h3>
                <p className="text-gray-600 mb-6 max-w-xs">
                  I'll analyze your data and help you clean, transform, and prepare it through conversation.
                </p>
                <button
                  onClick={() => fileInputRef.current?.click()}
                  disabled={uploading}
                  className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
                >
                  {uploading ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Upload className="w-4 h-4" />
                  )}
                  {uploading ? 'Uploading...' : 'Upload CSV'}
                </button>
              </div>
            ) : (
              <>
                {messages.map((msg) => (
                  <div
                    key={msg.id}
                    className={`${
                      msg.role === 'user' ? 'ml-8' : 'mr-8'
                    }`}
                  >
                    <div
                      className={`p-3 rounded-lg ${
                        msg.role === 'user'
                          ? 'bg-blue-600 text-white'
                          : 'bg-gray-100 text-gray-900'
                      }`}
                    >
                      <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
                    </div>
                  </div>
                ))}
                <div ref={chatEndRef} />
              </>
            )}
          </div>

          {/* Plan Preview */}
          {currentPlan && (
            <div className="border-t border-gray-200 p-4 bg-blue-50">
              <div className="flex items-center justify-between mb-2">
                <h3 className="font-medium text-gray-900">
                  Transformation Plan ({currentPlan.step_count} steps)
                </h3>
              </div>
              <div className="space-y-2 mb-3">
                {currentPlan.steps.map((step) => (
                  <div key={step.step_number} className="text-sm text-gray-700">
                    {step.step_number}. {step.explanation}
                  </div>
                ))}
              </div>
              <div className="flex gap-2">
                <button
                  onClick={handleApplyPlan}
                  disabled={applyingPlan}
                  className="flex-1 px-3 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg disabled:opacity-50"
                >
                  {applyingPlan ? 'Applying...' : 'Apply Plan'}
                </button>
                <button
                  onClick={handleClearPlan}
                  className="px-3 py-2 bg-gray-200 hover:bg-gray-300 text-gray-700 text-sm font-medium rounded-lg"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {/* Chat Input */}
          <div className="border-t border-gray-200 p-4">
            <form onSubmit={handleSendMessage} className="flex gap-2">
              <input
                type="text"
                value={inputMessage}
                onChange={(e) => setInputMessage(e.target.value)}
                placeholder={dataNodes.length === 0 ? 'Upload a CSV first...' : 'Describe your transformation...'}
                disabled={dataNodes.length === 0 || sending}
                className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent disabled:bg-gray-50 disabled:text-gray-400"
              />
              <button
                type="submit"
                disabled={!inputMessage.trim() || dataNodes.length === 0 || sending}
                className="p-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {sending ? (
                  <Loader2 className="w-5 h-5 animate-spin" />
                ) : (
                  <Send className="w-5 h-5" />
                )}
              </button>
            </form>
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleFileUpload}
              className="hidden"
            />
            {messages.length > 0 && (
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={uploading}
                className="mt-2 w-full px-3 py-2 text-sm text-gray-600 hover:bg-gray-100 rounded-lg transition-colors flex items-center justify-center gap-2"
              >
                {uploading ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Upload className="w-4 h-4" />
                )}
                Upload another file
              </button>
            )}
          </div>
        </div>

        {/* Right Panel - Node Graph */}
        <div className="flex-1 flex">
          <div className="flex-1 relative">
            {dataNodes.length === 0 ? (
              <div className="absolute inset-0 flex items-center justify-center text-gray-400">
                <p>Upload a CSV to see your transformation graph</p>
              </div>
            ) : (
              <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onNodeClick={handleNodeClick}
                nodeTypes={nodeTypes}
                fitView
                fitViewOptions={{ padding: 0.3 }}
                proOptions={{ hideAttribution: true }}
              >
                <Background color="#e5e7eb" gap={16} />
                <Controls position="bottom-right" />
              </ReactFlow>
            )}
          </div>

          {/* Node Detail Panel */}
          {selectedNodeId && selectedDataNode && sessionId && (
            <NodeDetailPanel
              sessionId={sessionId}
              node={selectedDataNode}
              onClose={() => setSelectedNodeId(null)}
              onDataRefresh={loadSessionData}
            />
          )}
        </div>
      </div>
    </div>
  )
}
