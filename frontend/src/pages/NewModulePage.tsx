import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { ArrowLeft, FileSpreadsheet, FileJson, FileText, Plug, Loader2 } from 'lucide-react'
import { api } from '../lib/api'

interface ModuleType {
  id: string
  name: string
  description: string
  icon: React.ReactNode
  available: boolean
}

const moduleTypes: ModuleType[] = [
  {
    id: 'csv',
    name: 'CSV Transformation',
    description: 'Clean, transform, and prepare CSV data through natural conversation',
    icon: <FileSpreadsheet className="w-8 h-8" />,
    available: true,
  },
  {
    id: 'excel',
    name: 'Excel Transformation',
    description: 'Transform Excel spreadsheets (.xlsx, .xls) - Coming Soon',
    icon: <FileSpreadsheet className="w-8 h-8" />,
    available: false,
  },
  {
    id: 'json',
    name: 'JSON Transformation',
    description: 'Transform and restructure JSON data files - Coming Soon',
    icon: <FileJson className="w-8 h-8" />,
    available: false,
  },
  {
    id: 'text',
    name: 'Text File Transformation',
    description: 'Transform text files (TXT, TSV, logs) - Coming Soon',
    icon: <FileText className="w-8 h-8" />,
    available: false,
  },
  {
    id: 'custom',
    name: 'Custom Integration',
    description: 'Connect to APIs and databases - Coming Soon',
    icon: <Plug className="w-8 h-8" />,
    available: false,
  },
]

export default function NewModulePage() {
  const navigate = useNavigate()
  const [creating, setCreating] = useState(false)
  const [error, setError] = useState('')

  const handleSelectType = async (type: ModuleType) => {
    if (!type.available) return

    setCreating(true)
    setError('')

    try {
      const data = await api.createSession()
      navigate(`/session/${data.session_id}`)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create module')
      setCreating(false)
    }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-6xl mx-auto px-4 py-4 flex items-center gap-4">
          <button
            onClick={() => navigate('/dashboard')}
            className="p-2 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-5 h-5 text-gray-600" />
          </button>
          <h1 className="text-xl font-bold text-gray-900">Create New Module</h1>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-4xl mx-auto px-4 py-8">
        <div className="text-center mb-8">
          <h2 className="text-2xl font-bold text-gray-900 mb-2">
            What type of data do you want to transform?
          </h2>
          <p className="text-gray-600">
            Select a module type to get started
          </p>
        </div>

        {error && (
          <div className="mb-6 p-4 bg-red-50 text-red-700 rounded-lg text-center">
            {error}
          </div>
        )}

        {creating && (
          <div className="flex items-center justify-center py-8">
            <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
            <span className="ml-3 text-gray-600">Creating module...</span>
          </div>
        )}

        {!creating && (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {moduleTypes.map((type) => (
              <button
                key={type.id}
                onClick={() => handleSelectType(type)}
                disabled={!type.available}
                className={`p-6 rounded-xl border-2 text-left transition-all ${
                  type.available
                    ? 'bg-white border-gray-200 hover:border-blue-500 hover:shadow-md cursor-pointer'
                    : 'bg-gray-50 border-gray-100 cursor-not-allowed opacity-60'
                }`}
              >
                <div
                  className={`w-14 h-14 rounded-xl flex items-center justify-center mb-4 ${
                    type.available
                      ? 'bg-blue-100 text-blue-600'
                      : 'bg-gray-200 text-gray-400'
                  }`}
                >
                  {type.icon}
                </div>
                <h3
                  className={`font-semibold mb-2 ${
                    type.available ? 'text-gray-900' : 'text-gray-500'
                  }`}
                >
                  {type.name}
                </h3>
                <p
                  className={`text-sm ${
                    type.available ? 'text-gray-600' : 'text-gray-400'
                  }`}
                >
                  {type.description}
                </p>
              </button>
            ))}
          </div>
        )}
      </main>
    </div>
  )
}
