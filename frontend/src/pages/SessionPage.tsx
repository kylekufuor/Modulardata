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
import { ArrowLeft, Send, Upload, Loader2 } from 'lucide-react'
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

  // Loading states
  const [loading, setLoading] = useState(true)
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

  const loadSessionData = async () => {
    if (!sessionId) return

    try {
      const history: HistoryResponse = await api.getHistory(sessionId)
      setDataNodes(history.nodes)
      setMessages(history.messages)
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

      // Add system message about upload
      const uploadMessage: ChatMessage = {
        id: `upload-${Date.now()}`,
        role: 'assistant',
        content: `Uploaded "${response.filename}" with ${response.profile.row_count} rows and ${response.profile.column_count} columns.`,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, uploadMessage])

      // Reload session data to get the new node
      await loadSessionData()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Upload failed')
    } finally {
      setUploading(false)
      if (fileInputRef.current) {
        fileInputRef.current.value = ''
      }
    }
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
        <h1 className="font-semibold text-gray-900">CSV Transformation</h1>
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
            {/* Welcome Message - Always shown first */}
            <div className="mr-8">
              <div className="p-3 rounded-lg bg-gray-100 text-gray-900">
                <p className="text-sm font-medium mb-2">Welcome to ModularData! 👋</p>
                <p className="text-sm whitespace-pre-wrap">
                  I'm your data transformation assistant. I help you clean, transform, and prepare your data through natural conversation.
                </p>
                {dataNodes.length === 0 && (
                  <p className="text-sm mt-2 text-gray-600">
                    Upload a CSV file to get started, and I'll help you explore and clean your data.
                  </p>
                )}
              </div>
            </div>

            {messages.length === 0 && dataNodes.length === 0 ? (
              <div className="text-center py-4">
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
                  Upload CSV
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
            />
          )}
        </div>
      </div>
    </div>
  )
}
