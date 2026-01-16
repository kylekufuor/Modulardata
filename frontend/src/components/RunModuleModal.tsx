import { useState, useRef, useCallback } from 'react'
import {
  X,
  Upload,
  Loader2,
  CheckCircle,
  AlertTriangle,
  XCircle,
  Download,
  FileSpreadsheet,
  ChevronDown,
  ChevronUp,
} from 'lucide-react'
import { api } from '../lib/api'

interface RunResult {
  run_id: string
  status: 'success' | 'failed' | 'pending' | 'warning_confirmed'
  confidence_score: number
  confidence_level: 'HIGH' | 'MEDIUM' | 'LOW' | 'NO_MATCH'
  input_rows?: number
  input_columns?: number
  output_rows?: number
  output_columns?: number
  error_message?: string
  requires_confirmation?: boolean
  column_mappings?: Array<{
    incoming_name: string
    contract_name: string
    match_type: string
    confidence: number
  }>
  discrepancies?: Array<{
    type: string
    severity: string
    column: string
    description: string
    suggestion?: string
  }>
  output_storage_path?: string
  duration_ms?: number
  message?: string
}

interface RunModuleModalProps {
  sessionId: string
  moduleName: string
  isOpen: boolean
  onClose: () => void
}

type Step = 'upload' | 'running' | 'result'

export default function RunModuleModal({
  sessionId,
  moduleName,
  isOpen,
  onClose,
}: RunModuleModalProps) {
  const [step, setStep] = useState<Step>('upload')
  const [file, setFile] = useState<File | null>(null)
  const [dragActive, setDragActive] = useState(false)
  const [running, setRunning] = useState(false)
  const [result, setResult] = useState<RunResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [showMappings, setShowMappings] = useState(false)
  const [downloading, setDownloading] = useState(false)

  const fileInputRef = useRef<HTMLInputElement>(null)

  const resetModal = useCallback(() => {
    setStep('upload')
    setFile(null)
    setResult(null)
    setError(null)
    setShowMappings(false)
  }, [])

  const handleClose = () => {
    resetModal()
    onClose()
  }

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true)
    } else if (e.type === 'dragleave') {
      setDragActive(false)
    }
  }

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault()
    e.stopPropagation()
    setDragActive(false)

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const droppedFile = e.dataTransfer.files[0]
      if (droppedFile.name.endsWith('.csv')) {
        setFile(droppedFile)
        setError(null)
      } else {
        setError('Please upload a CSV file')
      }
    }
  }

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const selectedFile = e.target.files[0]
      if (selectedFile.name.endsWith('.csv')) {
        setFile(selectedFile)
        setError(null)
      } else {
        setError('Please upload a CSV file')
      }
    }
  }

  const handleRun = async (force: boolean = false) => {
    if (!file) return

    setRunning(true)
    setStep('running')
    setError(null)

    try {
      const runResult = await api.runModule(sessionId, file, force)
      setResult(runResult)
      setStep('result')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Run failed')
      setStep('result')
    } finally {
      setRunning(false)
    }
  }

  const handleConfirmRun = async () => {
    if (!file || !result) return
    // Run again with force=true
    await handleRun(true)
  }

  const handleDownload = async () => {
    if (!result?.run_id) return

    setDownloading(true)
    try {
      await api.downloadRunOutput(sessionId, result.run_id)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Download failed')
    } finally {
      setDownloading(false)
    }
  }

  const handleRunAnother = () => {
    resetModal()
  }

  if (!isOpen) return null

  const getConfidenceColor = (level: string) => {
    switch (level) {
      case 'HIGH':
        return 'text-green-600 bg-green-100'
      case 'MEDIUM':
        return 'text-yellow-600 bg-yellow-100'
      case 'LOW':
        return 'text-orange-600 bg-orange-100'
      case 'NO_MATCH':
        return 'text-red-600 bg-red-100'
      default:
        return 'text-gray-600 bg-gray-100'
    }
  }

  const getStatusIcon = () => {
    if (!result) return null
    switch (result.status) {
      case 'success':
      case 'warning_confirmed':
        return <CheckCircle className="w-16 h-16 text-green-500" />
      case 'pending':
        return <AlertTriangle className="w-16 h-16 text-yellow-500" />
      case 'failed':
        return <XCircle className="w-16 h-16 text-red-500" />
      default:
        return null
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50"
        onClick={handleClose}
      />

      {/* Modal */}
      <div className="relative bg-white rounded-xl shadow-xl w-full max-w-lg mx-4 max-h-[90vh] overflow-hidden flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Run Module</h2>
            <p className="text-sm text-gray-500">{moduleName}</p>
          </div>
          <button
            onClick={handleClose}
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Upload Step */}
          {step === 'upload' && (
            <div>
              <p className="text-sm text-gray-600 mb-4">
                Upload a CSV file to run through this module's transformations.
              </p>

              {/* Drop Zone */}
              <div
                className={`border-2 border-dashed rounded-xl p-8 text-center transition-colors ${
                  dragActive
                    ? 'border-blue-500 bg-blue-50'
                    : file
                    ? 'border-green-500 bg-green-50'
                    : 'border-gray-300 hover:border-gray-400'
                }`}
                onDragEnter={handleDrag}
                onDragLeave={handleDrag}
                onDragOver={handleDrag}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  onChange={handleFileSelect}
                  className="hidden"
                />

                {file ? (
                  <div className="space-y-2">
                    <FileSpreadsheet className="w-12 h-12 text-green-500 mx-auto" />
                    <p className="font-medium text-gray-900">{file.name}</p>
                    <p className="text-sm text-gray-500">
                      {(file.size / 1024).toFixed(1)} KB
                    </p>
                    <button
                      onClick={(e) => {
                        e.stopPropagation()
                        setFile(null)
                      }}
                      className="text-sm text-blue-600 hover:text-blue-700"
                    >
                      Choose different file
                    </button>
                  </div>
                ) : (
                  <div className="space-y-2">
                    <Upload className="w-12 h-12 text-gray-400 mx-auto" />
                    <p className="font-medium text-gray-700">
                      Drop your CSV file here
                    </p>
                    <p className="text-sm text-gray-500">
                      or click to browse
                    </p>
                  </div>
                )}
              </div>

              {error && (
                <div className="mt-4 p-3 bg-red-50 text-red-700 text-sm rounded-lg">
                  {error}
                </div>
              )}
            </div>
          )}

          {/* Running Step */}
          {step === 'running' && (
            <div className="text-center py-8">
              <Loader2 className="w-16 h-16 text-blue-500 animate-spin mx-auto mb-4" />
              <h3 className="text-lg font-medium text-gray-900 mb-2">
                Running Module
              </h3>
              <p className="text-sm text-gray-500">
                Processing your file through the transformation pipeline...
              </p>
            </div>
          )}

          {/* Result Step */}
          {step === 'result' && (
            <div>
              {error && !result ? (
                <div className="text-center py-8">
                  <XCircle className="w-16 h-16 text-red-500 mx-auto mb-4" />
                  <h3 className="text-lg font-medium text-gray-900 mb-2">
                    Run Failed
                  </h3>
                  <p className="text-sm text-red-600">{error}</p>
                </div>
              ) : result ? (
                <div className="space-y-6">
                  {/* Status */}
                  <div className="text-center">
                    {getStatusIcon()}
                    <h3 className="text-lg font-medium text-gray-900 mt-4 mb-1">
                      {result.status === 'success' || result.status === 'warning_confirmed'
                        ? 'Transformation Complete'
                        : result.status === 'pending'
                        ? 'Confirmation Required'
                        : 'Transformation Failed'}
                    </h3>
                    {result.message && (
                      <p className="text-sm text-gray-500">{result.message}</p>
                    )}
                  </div>

                  {/* Confidence Badge */}
                  <div className="flex justify-center">
                    <span
                      className={`px-3 py-1 rounded-full text-sm font-medium ${getConfidenceColor(
                        result.confidence_level
                      )}`}
                    >
                      {result.confidence_level} ({result.confidence_score.toFixed(0)}%)
                    </span>
                  </div>

                  {/* Stats */}
                  {(result.status === 'success' || result.status === 'warning_confirmed') && (
                    <div className="grid grid-cols-2 gap-4 p-4 bg-gray-50 rounded-lg">
                      <div className="text-center">
                        <p className="text-2xl font-semibold text-gray-900">
                          {result.input_rows?.toLocaleString()}
                        </p>
                        <p className="text-xs text-gray-500">Input Rows</p>
                      </div>
                      <div className="text-center">
                        <p className="text-2xl font-semibold text-gray-900">
                          {result.output_rows?.toLocaleString()}
                        </p>
                        <p className="text-xs text-gray-500">Output Rows</p>
                      </div>
                      <div className="text-center">
                        <p className="text-2xl font-semibold text-gray-900">
                          {result.input_columns}
                        </p>
                        <p className="text-xs text-gray-500">Input Columns</p>
                      </div>
                      <div className="text-center">
                        <p className="text-2xl font-semibold text-gray-900">
                          {result.output_columns}
                        </p>
                        <p className="text-xs text-gray-500">Output Columns</p>
                      </div>
                    </div>
                  )}

                  {/* Error Message */}
                  {result.error_message && (
                    <div className="p-4 bg-red-50 rounded-lg">
                      <p className="text-sm text-red-700 whitespace-pre-wrap">
                        {result.error_message}
                      </p>
                    </div>
                  )}

                  {/* Discrepancies */}
                  {result.discrepancies && result.discrepancies.length > 0 && (
                    <div className="p-4 bg-yellow-50 rounded-lg">
                      <h4 className="font-medium text-yellow-800 mb-2">
                        Schema Differences
                      </h4>
                      <ul className="space-y-1 text-sm text-yellow-700">
                        {result.discrepancies.slice(0, 5).map((d, i) => (
                          <li key={i}>
                            <span className="font-medium">{d.column}:</span>{' '}
                            {d.description}
                          </li>
                        ))}
                        {result.discrepancies.length > 5 && (
                          <li className="text-yellow-600">
                            ...and {result.discrepancies.length - 5} more
                          </li>
                        )}
                      </ul>
                    </div>
                  )}

                  {/* Column Mappings (collapsible) */}
                  {result.column_mappings && result.column_mappings.length > 0 && (
                    <div>
                      <button
                        onClick={() => setShowMappings(!showMappings)}
                        className="flex items-center gap-2 text-sm text-gray-600 hover:text-gray-900"
                      >
                        {showMappings ? (
                          <ChevronUp className="w-4 h-4" />
                        ) : (
                          <ChevronDown className="w-4 h-4" />
                        )}
                        {showMappings ? 'Hide' : 'Show'} Column Mappings
                      </button>
                      {showMappings && (
                        <div className="mt-2 border rounded-lg overflow-hidden">
                          <table className="w-full text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-3 py-2 text-left text-gray-600">
                                  Your Column
                                </th>
                                <th className="px-3 py-2 text-left text-gray-600">
                                  Mapped To
                                </th>
                                <th className="px-3 py-2 text-right text-gray-600">
                                  Confidence
                                </th>
                              </tr>
                            </thead>
                            <tbody>
                              {result.column_mappings.map((m, i) => (
                                <tr
                                  key={i}
                                  className="border-t border-gray-100"
                                >
                                  <td className="px-3 py-2 font-mono text-xs">
                                    {m.incoming_name}
                                  </td>
                                  <td className="px-3 py-2 font-mono text-xs">
                                    {m.contract_name}
                                  </td>
                                  <td className="px-3 py-2 text-right">
                                    {(m.confidence * 100).toFixed(0)}%
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Duration */}
                  {result.duration_ms && (
                    <p className="text-xs text-gray-400 text-center">
                      Completed in {(result.duration_ms / 1000).toFixed(2)}s
                    </p>
                  )}
                </div>
              ) : null}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-200 bg-gray-50">
          {step === 'upload' && (
            <div className="flex justify-end gap-3">
              <button
                onClick={handleClose}
                className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-200 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={() => handleRun(false)}
                disabled={!file}
                className="px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed rounded-lg transition-colors"
              >
                Run Module
              </button>
            </div>
          )}

          {step === 'running' && (
            <div className="flex justify-end">
              <button
                onClick={handleClose}
                className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-200 rounded-lg transition-colors"
              >
                Cancel
              </button>
            </div>
          )}

          {step === 'result' && (
            <div className="flex justify-between gap-3">
              <button
                onClick={handleRunAnother}
                className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-200 rounded-lg transition-colors"
              >
                Run Another File
              </button>
              <div className="flex gap-3">
                {result?.requires_confirmation && (
                  <button
                    onClick={handleConfirmRun}
                    disabled={running}
                    className="px-4 py-2 text-sm font-medium text-white bg-yellow-600 hover:bg-yellow-700 disabled:bg-gray-300 rounded-lg transition-colors flex items-center gap-2"
                  >
                    {running ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <AlertTriangle className="w-4 h-4" />
                    )}
                    Confirm & Run
                  </button>
                )}
                {(result?.status === 'success' || result?.status === 'warning_confirmed') && (
                  <button
                    onClick={handleDownload}
                    disabled={downloading}
                    className="px-4 py-2 text-sm font-medium text-white bg-green-600 hover:bg-green-700 disabled:bg-gray-300 rounded-lg transition-colors flex items-center gap-2"
                  >
                    {downloading ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <Download className="w-4 h-4" />
                    )}
                    Download Output
                  </button>
                )}
                <button
                  onClick={handleClose}
                  className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-200 rounded-lg transition-colors"
                >
                  Close
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
