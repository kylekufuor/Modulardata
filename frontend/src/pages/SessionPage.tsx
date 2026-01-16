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
import { ArrowLeft, Send, Upload, Loader2, Pencil, Check, X, Zap, ListChecks, Rocket, Trash2 } from 'lucide-react'
import { api } from '../lib/api'
import type { Node, ChatMessage as ChatMessageType, HistoryResponse, UploadResponse, Plan } from '../types'
import DataNode from '../components/DataNode'
import NodeDetailPanel from '../components/NodeDetailPanel'
import ChatMessageBubble from '../components/ChatMessage'
import ThinkingIndicator from '../components/ThinkingIndicator'

type ChatMode = 'plan' | 'transform'

const nodeTypes = {
  dataNode: DataNode,
}

export default function SessionPage() {
  const { sessionId } = useParams<{ sessionId: string }>()
  const navigate = useNavigate()

  // State
  const [nodes, setNodes, onNodesChange] = useNodesState<FlowNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [messages, setMessages] = useState<ChatMessageType[]>([])
  const [inputMessage, setInputMessage] = useState('')
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [dataNodes, setDataNodes] = useState<Node[]>([])
  const [currentPlan, setCurrentPlan] = useState<Plan | null>(null)
  const [showPlanPreview, setShowPlanPreview] = useState(true)
  const [chatMode, setChatMode] = useState<ChatMode>('plan')
  const [moduleName, setModuleName] = useState<string>('')
  const [deployedNodeId, setDeployedNodeId] = useState<string | null>(null)
  const [deploying, setDeploying] = useState(false)

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
  const contextMenuRef = useRef<HTMLDivElement>(null)

  // Context menu state
  const [contextMenu, setContextMenu] = useState<{
    x: number
    y: number
    nodeId: string
    canDelete: boolean
  } | null>(null)
  const [deleteConfirm, setDeleteConfirm] = useState<string | null>(null)
  const [deleting, setDeleting] = useState(false)

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

  // Close context menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (contextMenuRef.current && !contextMenuRef.current.contains(event.target as globalThis.Node)) {
        setContextMenu(null)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const loadSessionData = async (preserveMessages = false) => {
    if (!sessionId) return

    try {
      // Fetch session details for module name and deployment status
      const sessionDetails = await api.getSession(sessionId)
      const filename = sessionDetails.original_filename || 'Untitled Module'
      setModuleName(filename)
      setDeployedNodeId(sessionDetails.deployed_node_id || null)

      const history: HistoryResponse = await api.getHistory(sessionId)
      setDataNodes(history.nodes)

      // Only update messages if not preserving (e.g., after file upload with welcome message)
      if (!preserveMessages) {
        const originalNode = history.nodes.find(n => !n.parent_id)
        const needsWelcomeMessage = history.nodes.length > 0 && (
          history.messages.length === 0 ||
          history.messages[0]?.role === 'user'  // First message is from user, no welcome exists
        )

        if (needsWelcomeMessage && originalNode) {
          // Generate welcome message and prepend it
          let welcomeMsg: ChatMessageType
          try {
            const profileData = await api.getNodeProfile(sessionId, originalNode.id)
            welcomeMsg = {
              id: `welcome-${Date.now()}`,
              role: 'assistant',
              content: buildWelcomeMessageFromProfile(filename, profileData),
              created_at: originalNode.created_at,
            }
          } catch {
            // If profile fetch fails, show simple welcome
            welcomeMsg = {
              id: `welcome-${Date.now()}`,
              role: 'assistant',
              content: `Welcome! Your data "${filename}" has been loaded with ${originalNode.row_count.toLocaleString()} rows and ${originalNode.column_count} columns.\n\nWhat would you like to do with this data?`,
              created_at: originalNode.created_at,
            }
          }
          // Prepend welcome message to existing messages
          setMessages([welcomeMsg, ...history.messages])
        } else {
          setMessages(history.messages)
        }
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
    setContextMenu(null) // Close context menu on click
  }, [])

  const handleNodeContextMenu = useCallback((event: React.MouseEvent, node: FlowNode) => {
    event.preventDefault()
    const targetNode = dataNodes.find(n => n.id === node.id)
    if (!targetNode) return

    // Can only delete current node that has a parent (not original)
    const canDelete = targetNode.is_current && targetNode.parent_id !== null

    setContextMenu({
      x: event.clientX,
      y: event.clientY,
      nodeId: node.id,
      canDelete,
    })
  }, [dataNodes])

  const handleDeleteNode = async () => {
    if (!sessionId || !deleteConfirm) return

    setDeleting(true)
    try {
      const result = await api.deleteNode(sessionId, deleteConfirm)

      // Add message to chat
      const deleteMsg: ChatMessageType = {
        id: `delete-${Date.now()}`,
        role: 'assistant',
        content: `🗑️ ${result.message} Reverted to previous version.`,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, deleteMsg])

      setDeleteConfirm(null)
      setContextMenu(null)
      await loadSessionData(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete node')
    } finally {
      setDeleting(false)
    }
  }

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file || !sessionId) return

    setUploading(true)
    setError('')

    try {
      const response: UploadResponse = await api.uploadFile(sessionId, file)

      // Build welcome message with data profiler
      const welcomeMessage = buildWelcomeMessage(response)

      const uploadMessage: ChatMessageType = {
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

  // Build welcome message from profile API response (for page reload) - card style
  const buildWelcomeMessageFromProfile = (filename: string, profileData: { row_count: number; column_count: number; profile?: { columns?: Array<{ name: string; semantic_type: string; null_count: number }> } }): string => {
    const { row_count, column_count, profile } = profileData
    const columns = profile?.columns || []

    const columnsWithIssues = columns.filter(col => col.null_count > 0)
      .sort((a, b) => b.null_count - a.null_count)
    const totalMissing = columnsWithIssues.reduce((sum, col) => sum + col.null_count, 0)

    let message = `📊 ${filename} loaded!\n\n`
    message += `   ${row_count.toLocaleString()} rows  •  ${column_count} columns\n\n`

    if (columnsWithIssues.length > 0) {
      const topIssue = columnsWithIssues[0]
      message += `⚠️ Found ${totalMissing} missing values across ${columnsWithIssues.length} columns\n`
      message += `   → ${topIssue.name} has the most (${topIssue.null_count} missing)\n\n`
      message += `How can I help clean this data?`
    } else {
      message += `✅ Data looks clean!\n\n`
      message += `What would you like to do with it?`
    }

    return message
  }

  // Build welcome message from upload response (for new upload) - card style
  const buildWelcomeMessage = (response: UploadResponse): string => {
    const { filename, profile } = response
    const { row_count, column_count, columns } = profile

    const columnsWithIssues = columns.filter(col => col.null_count > 0)
      .sort((a, b) => b.null_count - a.null_count)
    const totalMissing = columnsWithIssues.reduce((sum, col) => sum + col.null_count, 0)

    let message = `📊 ${filename} loaded!\n\n`
    message += `   ${row_count.toLocaleString()} rows  •  ${column_count} columns\n\n`

    if (columnsWithIssues.length > 0) {
      const topIssue = columnsWithIssues[0]
      message += `⚠️ Found ${totalMissing} missing values across ${columnsWithIssues.length} columns\n`
      message += `   → ${topIssue.name} has the most (${topIssue.null_count} missing)\n\n`
      message += `How can I help clean this data?`
    } else {
      message += `✅ Data looks clean!\n\n`
      message += `What would you like to do with it?`
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
    const tempUserMsg: ChatMessageType = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: userMessage,
      created_at: new Date().toISOString(),
    }
    setMessages((prev) => [...prev, tempUserMsg])

    try {
      const response = await api.sendMessage(sessionId, userMessage, chatMode)

      // Add assistant response
      const assistantMsg: ChatMessageType = {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        content: response.assistant_response,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, assistantMsg])

      // In transform mode, reload data if transformation was applied
      if (chatMode === 'transform' && response.new_node_id) {
        await loadSessionData(true) // preserve messages
        // Don't show plan preview in transform mode
        setShowPlanPreview(false)
      } else if (response.plan && response.plan.step_count > 0) {
        // Plan mode: show plan preview when steps are added
        setCurrentPlan(response.plan)
        setShowPlanPreview(true)
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
    const stepCount = currentPlan.step_count
    try {
      const result = await api.applyPlan(sessionId)

      // Check if the transformation succeeded
      if (!result.success) {
        const errorMsg: ChatMessageType = {
          id: `apply-error-${Date.now()}`,
          role: 'assistant',
          content: `❌ Transformation failed: ${result.error || result.message || 'Unknown error'}\n\nPlease try a different approach or modify your request.`,
          created_at: new Date().toISOString(),
        }
        setMessages((prev) => [...prev, errorMsg])
        setError(result.error || result.message || 'Transformation failed')
        return
      }

      // Add success message to chat
      const successMsg: ChatMessageType = {
        id: `apply-${Date.now()}`,
        role: 'assistant',
        content: `✅ Done! Applied ${stepCount} transformation${stepCount > 1 ? 's' : ''} to your data.\n\nYour data has been updated. What would you like to do next?`,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, successMsg])

      setCurrentPlan(null)
      setShowPlanPreview(false)
      await loadSessionData(true) // preserve messages
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

  const handleDeploy = async () => {
    if (!sessionId) return

    setDeploying(true)
    try {
      const result = await api.deployModule(sessionId)
      setDeployedNodeId(result.deployed_node_id)

      // Add success message to chat
      const successMsg: ChatMessageType = {
        id: `deploy-${Date.now()}`,
        role: 'assistant',
        content: `🚀 Module deployed successfully!\n\nYour transformation pipeline is now ready to run on new data. Go to the Dashboard to run this module on other CSV files.`,
        created_at: new Date().toISOString(),
      }
      setMessages((prev) => [...prev, successMsg])
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to deploy module')
    } finally {
      setDeploying(false)
    }
  }

  // Check if module can be deployed (needs at least one transformation)
  const canDeploy = dataNodes.length >= 2
  const isDeployed = deployedNodeId !== null
  // Check if current state matches deployed state
  const currentNodeId = dataNodes.find(n => n.is_current)?.id
  const isModified = isDeployed && currentNodeId !== deployedNodeId

  const selectedDataNode = dataNodes.find((n) => n.id === selectedNodeId)
  const selectedParentNode = selectedDataNode?.parent_id
    ? dataNodes.find((n) => n.id === selectedDataNode.parent_id)
    : null

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

        {/* Spacer to push deploy button to right */}
        <div className="flex-1" />

        {/* Deploy Button */}
        <button
          onClick={handleDeploy}
          disabled={!canDeploy || deploying || (isDeployed && !isModified)}
          className={`flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
            !canDeploy
              ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
              : isDeployed && !isModified
              ? 'bg-green-100 text-green-700 cursor-default'
              : 'bg-green-600 hover:bg-green-700 text-white'
          }`}
          title={
            !canDeploy
              ? 'Add at least one transformation to deploy'
              : isDeployed && !isModified
              ? 'Module is deployed'
              : isModified
              ? 'Redeploy to update the deployed version'
              : 'Deploy this module'
          }
        >
          {deploying ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <Rocket className="w-4 h-4" />
          )}
          {deploying
            ? 'Deploying...'
            : isDeployed && !isModified
            ? 'Deployed'
            : isModified
            ? 'Redeploy'
            : 'Deploy'}
        </button>
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
                  <ChatMessageBubble key={msg.id} message={msg} />
                ))}
                {sending && <ThinkingIndicator />}
                <div ref={chatEndRef} />
              </>
            )}
          </div>

          {/* Plan Preview - only show when there are steps */}
          {currentPlan && showPlanPreview && currentPlan.step_count > 0 && (
            <div className={`border-t p-4 transition-colors ${
              currentPlan.step_count >= 3
                ? 'bg-gradient-to-r from-blue-50 to-emerald-50 border-blue-300'
                : 'bg-blue-50 border-gray-200'
            }`}>
              <div className="flex items-center justify-between mb-2">
                <div className="flex items-center gap-2">
                  <h3 className="font-medium text-gray-900">
                    Transformation Plan
                  </h3>
                  <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${
                    currentPlan.step_count >= 3
                      ? 'bg-emerald-100 text-emerald-700'
                      : 'bg-blue-100 text-blue-700'
                  }`}>
                    {currentPlan.step_count} {currentPlan.step_count === 1 ? 'step' : 'steps'}
                    {currentPlan.step_count >= 3 && ' ready!'}
                  </span>
                </div>
                <button
                  onClick={() => setShowPlanPreview(false)}
                  className="text-gray-400 hover:text-gray-600 p-1"
                  title="Dismiss"
                >
                  <X className="w-4 h-4" />
                </button>
              </div>
              <div className="space-y-2 mb-3 max-h-32 overflow-y-auto">
                {currentPlan.steps.map((step) => (
                  <div key={step.step_number} className="text-sm text-gray-700 flex items-start gap-2">
                    <span className="text-gray-400 font-mono text-xs mt-0.5">{step.step_number}.</span>
                    <span>{step.explanation}</span>
                  </div>
                ))}
              </div>
              <div className="flex flex-col gap-2">
                <div className="flex gap-2">
                  <button
                    onClick={handleApplyPlan}
                    disabled={applyingPlan}
                    className={`flex-1 px-3 py-2 text-white text-sm font-medium rounded-lg disabled:opacity-50 transition-all ${
                      currentPlan.step_count >= 3
                        ? 'bg-emerald-600 hover:bg-emerald-700 shadow-md shadow-emerald-200'
                        : 'bg-blue-600 hover:bg-blue-700'
                    }`}
                  >
                    {applyingPlan ? (
                      <span className="flex items-center justify-center gap-2">
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Applying...
                      </span>
                    ) : (
                      <span className="flex items-center justify-center gap-2">
                        <Zap className="w-4 h-4" />
                        Apply {currentPlan.step_count >= 3 ? 'All' : 'Plan'}
                      </span>
                    )}
                  </button>
                  <button
                    onClick={() => setShowPlanPreview(false)}
                    className="px-3 py-2 bg-gray-100 hover:bg-gray-200 text-gray-700 text-sm font-medium rounded-lg border border-gray-300"
                    title="Dismiss and keep adding transformations"
                  >
                    Keep Adding
                  </button>
                </div>
                <button
                  onClick={handleClearPlan}
                  className="text-xs text-gray-500 hover:text-gray-700 hover:underline self-center"
                >
                  Clear all steps
                </button>
              </div>
            </div>
          )}

          {/* Chat Input */}
          <div className="border-t border-gray-200 p-4 space-y-3">
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

            {/* Mode Toggle & Actions */}
            <div className="flex items-center justify-between">
              {/* Mode Toggle */}
              <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1">
                <button
                  onClick={() => setChatMode('plan')}
                  className={`flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
                    chatMode === 'plan'
                      ? 'bg-white text-blue-600 shadow-sm'
                      : 'text-gray-500 hover:text-gray-700'
                  }`}
                  title="Preview transformations before applying"
                >
                  <ListChecks className="w-3.5 h-3.5" />
                  Plan
                </button>
                <button
                  onClick={() => setChatMode('transform')}
                  className={`flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md transition-colors ${
                    chatMode === 'transform'
                      ? 'bg-white text-emerald-600 shadow-sm'
                      : 'text-gray-500 hover:text-gray-700'
                  }`}
                  title="Execute transformations immediately"
                >
                  <Zap className="w-3.5 h-3.5" />
                  Transform
                </button>
              </div>

              {/* Upload Button */}
              {messages.length > 0 && (
                <button
                  onClick={() => fileInputRef.current?.click()}
                  disabled={uploading}
                  className="text-xs text-gray-500 hover:text-gray-700 flex items-center gap-1"
                >
                  {uploading ? (
                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                  ) : (
                    <Upload className="w-3.5 h-3.5" />
                  )}
                  Upload file
                </button>
              )}
            </div>

            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              onChange={handleFileUpload}
              className="hidden"
            />
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
                onNodeContextMenu={handleNodeContextMenu}
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
              parentNode={selectedParentNode}
              onClose={() => setSelectedNodeId(null)}
              onDataRefresh={loadSessionData}
            />
          )}
        </div>
      </div>

      {/* Node Context Menu */}
      {contextMenu && (
        <div
          ref={contextMenuRef}
          className="fixed bg-white rounded-lg shadow-lg border border-gray-200 py-1 z-50"
          style={{ top: contextMenu.y, left: contextMenu.x }}
        >
          <button
            onClick={() => {
              if (contextMenu.canDelete) {
                setDeleteConfirm(contextMenu.nodeId)
                setContextMenu(null)
              }
            }}
            disabled={!contextMenu.canDelete}
            className={`w-full px-4 py-2 text-left text-sm flex items-center gap-2 ${
              contextMenu.canDelete
                ? 'text-red-600 hover:bg-red-50'
                : 'text-gray-400 cursor-not-allowed'
            }`}
            title={
              !contextMenu.canDelete
                ? 'Only the current (latest) transformation can be deleted'
                : 'Delete this transformation'
            }
          >
            <Trash2 className="w-4 h-4" />
            Delete
          </button>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {deleteConfirm && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl p-6 max-w-sm mx-4 shadow-xl">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">
              Delete Transformation?
            </h3>
            <p className="text-gray-600 mb-4">
              This will permanently delete this transformation and revert to the previous version.
              This action cannot be undone.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                onClick={() => setDeleteConfirm(null)}
                disabled={deleting}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                onClick={handleDeleteNode}
                disabled={deleting}
                className="px-4 py-2 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg disabled:opacity-50 flex items-center gap-2"
              >
                {deleting ? (
                  <>
                    <Loader2 className="w-4 h-4 animate-spin" />
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4" />
                    Delete
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
