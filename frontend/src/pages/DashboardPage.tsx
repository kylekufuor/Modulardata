import { useEffect, useState, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { Plus, FileSpreadsheet, LogOut, Loader2, Pencil, Check, X, MoreVertical, Trash2, Play, Rocket } from 'lucide-react'
import { useAuth } from '../hooks/useAuth'
import { api } from '../lib/api'
import type { Session } from '../types'
import RunModuleModal from '../components/RunModuleModal'

export default function DashboardPage() {
  const [modules, setModules] = useState<Session[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [editingId, setEditingId] = useState<string | null>(null)
  const [editName, setEditName] = useState('')
  const [saving, setSaving] = useState(false)
  const [menuOpenId, setMenuOpenId] = useState<string | null>(null)
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [runModalModule, setRunModalModule] = useState<Session | null>(null)
  const [deploying, setDeploying] = useState<string | null>(null)
  const editInputRef = useRef<HTMLInputElement>(null)
  const menuRef = useRef<HTMLDivElement>(null)

  const { user, signOut } = useAuth()
  const navigate = useNavigate()

  useEffect(() => {
    loadModules()
  }, [])

  const loadModules = async () => {
    try {
      const data = await api.listSessions()
      setModules(data.sessions)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load modules')
    } finally {
      setLoading(false)
    }
  }

  // Focus input when editing starts
  useEffect(() => {
    if (editingId && editInputRef.current) {
      editInputRef.current.focus()
      editInputRef.current.select()
    }
  }, [editingId])

  // Close menu when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setMenuOpenId(null)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleNewModule = () => {
    navigate('/new-module')
  }

  const toggleMenu = (e: React.MouseEvent, sessionId: string) => {
    e.stopPropagation()
    setMenuOpenId(menuOpenId === sessionId ? null : sessionId)
  }

  const startEditing = (e: React.MouseEvent, module: Session) => {
    e.stopPropagation()
    setMenuOpenId(null)
    setEditingId(module.session_id)
    setEditName(module.original_filename || 'Untitled Module')
  }

  const handleDeleteClick = (e: React.MouseEvent, sessionId: string) => {
    e.stopPropagation()
    setMenuOpenId(null)
    setDeleteConfirmId(sessionId)
  }

  const cancelDelete = (e: React.MouseEvent) => {
    e.stopPropagation()
    setDeleteConfirmId(null)
  }

  const confirmDelete = async (e: React.MouseEvent, sessionId: string) => {
    e.stopPropagation()
    setDeleting(true)
    try {
      await api.deleteSession(sessionId)
      setModules(modules.filter(m => m.session_id !== sessionId))
      setDeleteConfirmId(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete module')
    } finally {
      setDeleting(false)
    }
  }

  const handleRunClick = (e: React.MouseEvent, module: Session) => {
    e.stopPropagation()
    setMenuOpenId(null)
    setRunModalModule(module)
  }

  const handleDeployClick = async (e: React.MouseEvent, sessionId: string) => {
    e.stopPropagation()
    setMenuOpenId(null)
    setDeploying(sessionId)
    try {
      const result = await api.deployModule(sessionId)
      setModules(modules.map(m =>
        m.session_id === sessionId
          ? {
              ...m,
              status: 'deployed' as const,
              deployed_node_id: result.deployed_node_id,
              deployed_at: result.deployed_at,
            }
          : m
      ))
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to deploy module')
    } finally {
      setDeploying(null)
    }
  }

  const cancelEditing = (e: React.MouseEvent) => {
    e.stopPropagation()
    setEditingId(null)
    setEditName('')
  }

  const saveModuleName = async (e: React.MouseEvent, sessionId: string) => {
    e.stopPropagation()
    if (!editName.trim()) return

    setSaving(true)
    try {
      await api.renameModule(sessionId, editName.trim())
      setModules(modules.map(m =>
        m.session_id === sessionId
          ? { ...m, original_filename: editName.trim() }
          : m
      ))
      setEditingId(null)
      setEditName('')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to rename module')
    } finally {
      setSaving(false)
    }
  }

  const handleEditKeyDown = (e: React.KeyboardEvent, sessionId: string) => {
    if (e.key === 'Enter') {
      e.preventDefault()
      saveModuleName(e as unknown as React.MouseEvent, sessionId)
    } else if (e.key === 'Escape') {
      setEditingId(null)
      setEditName('')
    }
  }

  const handleSignOut = async () => {
    await signOut()
    navigate('/auth')
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    })
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center justify-between">
          <h1 className="text-xl font-bold text-gray-900">ModularData</h1>
          <div className="flex items-center gap-4">
            <span className="text-sm text-gray-600">{user?.email}</span>
            <button
              onClick={handleSignOut}
              className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
              title="Sign out"
            >
              <LogOut className="w-5 h-5" />
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-6xl mx-auto px-4 py-8">
        {/* Page Title */}
        <div className="flex items-center justify-between mb-8">
          <div>
            <h2 className="text-2xl font-bold text-gray-900">Modules</h2>
          </div>
          <button
            onClick={handleNewModule}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
          >
            <Plus className="w-5 h-5" />
            New Module
          </button>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 text-red-700 rounded-lg">
            {error}
          </div>
        )}

        {/* Modules List */}
        {loading ? (
          <div className="flex items-center justify-center py-12">
            <Loader2 className="w-8 h-8 animate-spin text-gray-400" />
          </div>
        ) : modules.length === 0 ? (
          <div className="text-center py-12 bg-white rounded-xl border border-gray-200">
            <FileSpreadsheet className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <h3 className="text-lg font-medium text-gray-900 mb-2">No modules yet</h3>
            <p className="text-gray-600 mb-6">Create your first module to get started</p>
            <button
              onClick={handleNewModule}
              className="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
            >
              <Plus className="w-5 h-5" />
              New Module
            </button>
          </div>
        ) : (
          <div className="grid gap-4">
            {modules.map((module) => (
              <div
                key={module.session_id}
                onClick={() => navigate(`/session/${module.session_id}`)}
                className="group bg-white rounded-xl border border-gray-200 p-4 hover:border-blue-300 hover:shadow-sm cursor-pointer transition-all"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                      <FileSpreadsheet className="w-5 h-5 text-blue-600" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        {editingId === module.session_id ? (
                          <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
                            <input
                              ref={editInputRef}
                              type="text"
                              value={editName}
                              onChange={(e) => setEditName(e.target.value)}
                              onKeyDown={(e) => handleEditKeyDown(e, module.session_id)}
                              className="px-2 py-1 text-sm border border-gray-300 rounded focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                              disabled={saving}
                            />
                            <button
                              onClick={(e) => saveModuleName(e, module.session_id)}
                              disabled={saving || !editName.trim()}
                              className="p-1 text-green-600 hover:bg-green-50 rounded disabled:opacity-50"
                            >
                              {saving ? (
                                <Loader2 className="w-4 h-4 animate-spin" />
                              ) : (
                                <Check className="w-4 h-4" />
                              )}
                            </button>
                            <button
                              onClick={cancelEditing}
                              disabled={saving}
                              className="p-1 text-gray-500 hover:bg-gray-100 rounded disabled:opacity-50"
                            >
                              <X className="w-4 h-4" />
                            </button>
                          </div>
                        ) : (
                          <h3 className="font-medium text-gray-900">
                            {module.original_filename || 'Untitled Module'}
                          </h3>
                        )}
                      </div>
                      <div className="flex items-center gap-2 mt-0.5">
                        <span className="text-xs text-blue-600 font-medium">CSV Transformation</span>
                        <span className="text-gray-300">•</span>
                        <span className="text-sm text-gray-500">
                          Created {formatDate(module.created_at)}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {deploying === module.session_id ? (
                      <span className="px-2 py-1 text-xs font-medium rounded-full bg-blue-100 text-blue-700 flex items-center gap-1">
                        <Loader2 className="w-3 h-3 animate-spin" />
                        Deploying...
                      </span>
                    ) : (
                      <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                        module.status === 'deployed'
                          ? 'bg-green-100 text-green-700'
                          : module.deployed_node_id
                          ? 'bg-yellow-100 text-yellow-700'
                          : 'bg-gray-100 text-gray-600'
                      }`}>
                        {module.status === 'deployed'
                          ? 'Deployed'
                          : module.deployed_node_id
                          ? 'Modified'
                          : 'Draft'}
                      </span>
                    )}

                    {/* 3-dot menu */}
                    <div className="relative" ref={menuOpenId === module.session_id ? menuRef : null}>
                      <button
                        onClick={(e) => toggleMenu(e, module.session_id)}
                        className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg opacity-0 group-hover:opacity-100 transition-opacity"
                      >
                        <MoreVertical className="w-4 h-4" />
                      </button>

                      {/* Dropdown menu */}
                      {menuOpenId === module.session_id && (
                        <div className="absolute right-0 top-8 w-36 bg-white rounded-lg shadow-lg border border-gray-200 py-1 z-10">
                          {module.deployed_node_id && (
                            <button
                              onClick={(e) => handleRunClick(e, module)}
                              className="w-full px-3 py-2 text-left text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2"
                            >
                              <Play className="w-4 h-4" />
                              Run
                            </button>
                          )}
                          {module.status !== 'deployed' && (
                            <button
                              onClick={(e) => handleDeployClick(e, module.session_id)}
                              className="w-full px-3 py-2 text-left text-sm text-green-700 hover:bg-green-50 flex items-center gap-2"
                            >
                              <Rocket className="w-4 h-4" />
                              {module.deployed_node_id ? 'Redeploy' : 'Deploy'}
                            </button>
                          )}
                          <button
                            onClick={(e) => startEditing(e, module)}
                            className="w-full px-3 py-2 text-left text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2"
                          >
                            <Pencil className="w-4 h-4" />
                            Rename
                          </button>
                          <button
                            onClick={(e) => handleDeleteClick(e, module.session_id)}
                            className="w-full px-3 py-2 text-left text-sm text-red-600 hover:bg-red-50 flex items-center gap-2"
                          >
                            <Trash2 className="w-4 h-4" />
                            Delete
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                </div>

                {/* Delete confirmation */}
                {deleteConfirmId === module.session_id && (
                  <div className="mt-3 pt-3 border-t border-gray-200" onClick={(e) => e.stopPropagation()}>
                    <p className="text-sm text-gray-600 mb-3">
                      Are you sure you want to delete this module? This action cannot be undone.
                    </p>
                    <div className="flex gap-2">
                      <button
                        onClick={(e) => confirmDelete(e, module.session_id)}
                        disabled={deleting}
                        className="px-3 py-1.5 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg disabled:opacity-50"
                      >
                        {deleting ? 'Deleting...' : 'Delete'}
                      </button>
                      <button
                        onClick={cancelDelete}
                        disabled={deleting}
                        className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg disabled:opacity-50"
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </main>

      {/* Run Module Modal */}
      {runModalModule && (
        <RunModuleModal
          sessionId={runModalModule.session_id}
          moduleName={runModalModule.original_filename || 'Untitled Module'}
          isOpen={true}
          onClose={() => setRunModalModule(null)}
        />
      )}
    </div>
  )
}
