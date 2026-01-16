import { useState } from 'react'
import { Upload, Database, HelpCircle, Loader2, X, ChevronRight } from 'lucide-react'
import { api } from '../lib/api'

interface Sample {
  id: string
  name: string
  filename: string
  description: string
  issues: string[]
  rows: number
  columns: number
}

interface QuickActionsProps {
  sessionId: string
  onUploadClick: () => void
  onSampleUploaded: () => void
  uploading: boolean
}

type ActionState = 'initial' | 'samples' | 'capabilities'

export default function QuickActions({
  sessionId,
  onUploadClick,
  onSampleUploaded,
  uploading,
}: QuickActionsProps) {
  const [actionState, setActionState] = useState<ActionState>('initial')
  const [samples, setSamples] = useState<Sample[]>([])
  const [loadingSamples, setLoadingSamples] = useState(false)
  const [uploadingSample, setUploadingSample] = useState<string | null>(null)

  const handleTrySampleData = async () => {
    setLoadingSamples(true)
    try {
      const data = await api.listSamples()
      setSamples(data.samples || [])
      setActionState('samples')
    } catch (err) {
      console.error('Failed to load samples:', err)
    } finally {
      setLoadingSamples(false)
    }
  }

  const handleSelectSample = async (sampleId: string) => {
    setUploadingSample(sampleId)
    try {
      await api.uploadSample(sessionId, sampleId)
      onSampleUploaded()
    } catch (err) {
      console.error('Failed to upload sample:', err)
    } finally {
      setUploadingSample(null)
    }
  }

  const handleShowCapabilities = () => {
    setActionState('capabilities')
  }

  const handleBack = () => {
    setActionState('initial')
  }

  // Initial state - welcome with quick actions
  if (actionState === 'initial') {
    return (
      <div className="flex flex-col items-center justify-center h-full text-center px-4">
        {/* Chat bubble style welcome */}
        <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-2xl p-6 mb-6 max-w-sm border border-blue-100">
          <p className="text-gray-800 text-lg mb-2">
            Let's get started! What data do you need to clean?
          </p>
          <p className="text-gray-600 text-sm">
            I can help you transform messy CSVs, standardize formats, remove duplicates, and more.
          </p>
        </div>

        {/* Quick action buttons */}
        <div className="flex flex-col gap-3 w-full max-w-xs">
          <button
            onClick={onUploadClick}
            disabled={uploading}
            className="flex items-center gap-3 px-4 py-3 bg-blue-600 hover:bg-blue-700 text-white rounded-xl transition-colors disabled:opacity-50 shadow-sm"
          >
            {uploading ? (
              <Loader2 className="w-5 h-5 animate-spin" />
            ) : (
              <Upload className="w-5 h-5" />
            )}
            <span className="font-medium">{uploading ? 'Uploading...' : 'Upload a CSV file'}</span>
          </button>

          <button
            onClick={handleTrySampleData}
            disabled={loadingSamples || uploading}
            className="flex items-center gap-3 px-4 py-3 bg-white hover:bg-gray-50 text-gray-700 rounded-xl transition-colors border border-gray-200 disabled:opacity-50"
          >
            {loadingSamples ? (
              <Loader2 className="w-5 h-5 animate-spin text-gray-400" />
            ) : (
              <Database className="w-5 h-5 text-gray-500" />
            )}
            <span className="font-medium">Try with sample data</span>
          </button>

          <button
            onClick={handleShowCapabilities}
            disabled={uploading}
            className="flex items-center gap-3 px-4 py-3 bg-white hover:bg-gray-50 text-gray-700 rounded-xl transition-colors border border-gray-200 disabled:opacity-50"
          >
            <HelpCircle className="w-5 h-5 text-gray-500" />
            <span className="font-medium">What can you help with?</span>
          </button>
        </div>
      </div>
    )
  }

  // Sample data picker
  if (actionState === 'samples') {
    return (
      <div className="flex flex-col h-full">
        {/* Header */}
        <div className="flex items-center gap-2 px-4 py-3 border-b border-gray-100">
          <button
            onClick={handleBack}
            className="p-1 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <X className="w-4 h-4 text-gray-500" />
          </button>
          <h3 className="font-medium text-gray-900">Choose sample data</h3>
        </div>

        {/* Sample list */}
        <div className="flex-1 overflow-y-auto p-4 space-y-3">
          {samples.map((sample) => (
            <button
              key={sample.id}
              onClick={() => handleSelectSample(sample.id)}
              disabled={uploadingSample !== null}
              className="w-full text-left p-4 bg-white hover:bg-gray-50 rounded-xl border border-gray-200 transition-colors disabled:opacity-50"
            >
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1">
                    <span className="font-medium text-gray-900">{sample.name}</span>
                    <span className="text-xs text-gray-500">
                      {sample.rows} rows × {sample.columns} cols
                    </span>
                  </div>
                  <p className="text-sm text-gray-600 mb-2">{sample.description}</p>
                  <div className="flex flex-wrap gap-1">
                    {sample.issues.slice(0, 3).map((issue, idx) => (
                      <span
                        key={idx}
                        className="text-xs px-2 py-0.5 bg-amber-50 text-amber-700 rounded-full"
                      >
                        {issue}
                      </span>
                    ))}
                  </div>
                </div>
                <div className="flex-shrink-0">
                  {uploadingSample === sample.id ? (
                    <Loader2 className="w-5 h-5 animate-spin text-blue-600" />
                  ) : (
                    <ChevronRight className="w-5 h-5 text-gray-400" />
                  )}
                </div>
              </div>
            </button>
          ))}
        </div>
      </div>
    )
  }

  // Capabilities explanation
  if (actionState === 'capabilities') {
    return (
      <div className="flex flex-col h-full">
        {/* Header */}
        <div className="flex items-center gap-2 px-4 py-3 border-b border-gray-100">
          <button
            onClick={handleBack}
            className="p-1 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <X className="w-4 h-4 text-gray-500" />
          </button>
          <h3 className="font-medium text-gray-900">What I can help with</h3>
        </div>

        {/* Capabilities content */}
        <div className="flex-1 overflow-y-auto p-4">
          <div className="bg-gradient-to-br from-blue-50 to-indigo-50 rounded-2xl p-5 border border-blue-100">
            <p className="text-gray-800 mb-4">
              I'm your data cleaning assistant! Here's what I can do:
            </p>

            <div className="space-y-3">
              <div className="flex items-start gap-3">
                <span className="text-lg">🧹</span>
                <div>
                  <p className="font-medium text-gray-800">Clean messy data</p>
                  <p className="text-sm text-gray-600">Remove duplicates, fill missing values, trim whitespace</p>
                </div>
              </div>

              <div className="flex items-start gap-3">
                <span className="text-lg">📝</span>
                <div>
                  <p className="font-medium text-gray-800">Standardize formats</p>
                  <p className="text-sm text-gray-600">Fix phone numbers, dates, names, addresses</p>
                </div>
              </div>

              <div className="flex items-start gap-3">
                <span className="text-lg">🔄</span>
                <div>
                  <p className="font-medium text-gray-800">Transform columns</p>
                  <p className="text-sm text-gray-600">Rename, split, merge, convert data types</p>
                </div>
              </div>

              <div className="flex items-start gap-3">
                <span className="text-lg">🚀</span>
                <div>
                  <p className="font-medium text-gray-800">Deploy & reuse</p>
                  <p className="text-sm text-gray-600">Save your transformations and run on new files</p>
                </div>
              </div>
            </div>

            <p className="text-gray-600 text-sm mt-4">
              Just describe what you need in plain English, and I'll help you do it!
            </p>
          </div>

          {/* CTA */}
          <div className="mt-4 flex flex-col gap-2">
            <button
              onClick={onUploadClick}
              disabled={uploading}
              className="flex items-center justify-center gap-2 px-4 py-3 bg-blue-600 hover:bg-blue-700 text-white rounded-xl transition-colors disabled:opacity-50"
            >
              {uploading ? (
                <Loader2 className="w-5 h-5 animate-spin" />
              ) : (
                <Upload className="w-5 h-5" />
              )}
              <span className="font-medium">{uploading ? 'Uploading...' : 'Upload a CSV to start'}</span>
            </button>
            <button
              onClick={handleTrySampleData}
              disabled={loadingSamples || uploading}
              className="text-sm text-gray-500 hover:text-gray-700"
            >
              or try with sample data
            </button>
          </div>
        </div>
      </div>
    )
  }

  return null
}
