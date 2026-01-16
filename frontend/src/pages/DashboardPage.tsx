import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Plus, FileSpreadsheet, LogOut, Loader2 } from 'lucide-react'
import { useAuth } from '../hooks/useAuth'
import { api } from '../lib/api'
import type { Session } from '../types'

export default function DashboardPage() {
  const [modules, setModules] = useState<Session[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

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

  const handleNewModule = () => {
    navigate('/new-module')
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
                className="bg-white rounded-xl border border-gray-200 p-4 hover:border-blue-300 hover:shadow-sm cursor-pointer transition-all"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                      <FileSpreadsheet className="w-5 h-5 text-blue-600" />
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <h3 className="font-medium text-gray-900">
                          {module.original_filename || 'Untitled Module'}
                        </h3>
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
                    <span className={`px-2 py-1 text-xs font-medium rounded-full ${
                      module.status === 'active'
                        ? 'bg-green-100 text-green-700'
                        : 'bg-gray-100 text-gray-600'
                    }`}>
                      {module.status}
                    </span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </main>
    </div>
  )
}
