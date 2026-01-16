import { AlertTriangle, X, ArrowRight } from 'lucide-react'
import type { RiskPreview } from '../types'

interface RiskConfirmationModalProps {
  isOpen: boolean
  onClose: () => void
  onConfirm: () => void
  riskLevel: 'moderate' | 'high'
  riskReasons: string[]
  riskPreview?: RiskPreview
  isConfirming?: boolean
}

export default function RiskConfirmationModal({
  isOpen,
  onClose,
  onConfirm,
  riskLevel,
  riskReasons,
  riskPreview,
  isConfirming = false,
}: RiskConfirmationModalProps) {
  if (!isOpen) return null

  const isHighRisk = riskLevel === 'high'

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-xl shadow-xl max-w-md w-full overflow-hidden">
        {/* Header */}
        <div className={`px-6 py-4 ${isHighRisk ? 'bg-red-50' : 'bg-amber-50'}`}>
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-full ${isHighRisk ? 'bg-red-100' : 'bg-amber-100'}`}>
                <AlertTriangle className={`w-5 h-5 ${isHighRisk ? 'text-red-600' : 'text-amber-600'}`} />
              </div>
              <div>
                <h3 className={`font-semibold ${isHighRisk ? 'text-red-900' : 'text-amber-900'}`}>
                  {isHighRisk ? 'Significant Change' : 'Confirm Changes'}
                </h3>
                <p className={`text-sm ${isHighRisk ? 'text-red-700' : 'text-amber-700'}`}>
                  This operation will modify your data
                </p>
              </div>
            </div>
            <button
              onClick={onClose}
              className="p-1 hover:bg-gray-200 rounded-full transition-colors"
            >
              <X className="w-5 h-5 text-gray-500" />
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="px-6 py-4 space-y-4">
          {/* Risk Reasons */}
          <div className="space-y-2">
            {riskReasons.map((reason, index) => (
              <div
                key={index}
                className="flex items-start gap-2 text-sm text-gray-700"
              >
                <span className={`mt-0.5 ${isHighRisk ? 'text-red-500' : 'text-amber-500'}`}>•</span>
                <span>{reason}</span>
              </div>
            ))}
          </div>

          {/* Preview Stats */}
          {riskPreview && (riskPreview.rows_removed || riskPreview.columns_removed) && (
            <div className="bg-gray-50 rounded-lg p-4 space-y-3">
              <div className="text-sm font-medium text-gray-700">Impact Preview</div>

              {/* Row changes */}
              {riskPreview.rows_after !== undefined && (
                <div className="flex items-center gap-2 text-sm">
                  <span className="text-gray-600">Rows:</span>
                  <span className="font-mono">{riskPreview.rows_before.toLocaleString()}</span>
                  <ArrowRight className="w-4 h-4 text-gray-400" />
                  <span className="font-mono font-medium">
                    {riskPreview.rows_after.toLocaleString()}
                  </span>
                  {riskPreview.rows_removed && riskPreview.rows_removed > 0 && (
                    <span className="text-red-600 text-xs">
                      (-{riskPreview.rows_removed.toLocaleString()})
                    </span>
                  )}
                </div>
              )}

              {/* Column changes */}
              {riskPreview.columns_removed && riskPreview.columns_removed.length > 0 && (
                <div className="space-y-1">
                  <div className="text-sm text-gray-600">Columns to remove:</div>
                  <div className="flex flex-wrap gap-1">
                    {riskPreview.columns_removed.map((col) => (
                      <span
                        key={col}
                        className="px-2 py-0.5 bg-red-100 text-red-700 rounded text-xs font-mono"
                      >
                        {col}
                      </span>
                    ))}
                  </div>
                </div>
              )}

              {/* Sample of removed rows */}
              {riskPreview.sample_removed && riskPreview.sample_removed.length > 0 && (
                <div className="space-y-1">
                  <div className="text-sm text-gray-600">Sample rows that will be removed:</div>
                  <div className="max-h-32 overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="bg-gray-100">
                          {Object.keys(riskPreview.sample_removed[0]).slice(0, 4).map((key) => (
                            <th key={key} className="px-2 py-1 text-left font-medium text-gray-600">
                              {key}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {riskPreview.sample_removed.map((row, i) => (
                          <tr key={i} className="border-t border-gray-100">
                            {Object.entries(row).slice(0, 4).map(([key, value]) => (
                              <td key={key} className="px-2 py-1 text-gray-700 truncate max-w-[100px]">
                                {String(value ?? '')}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 bg-gray-50 flex items-center justify-end gap-3">
          <button
            onClick={onClose}
            disabled={isConfirming}
            className="px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-200 rounded-lg transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={isConfirming}
            className={`px-4 py-2 text-sm font-medium text-white rounded-lg transition-colors disabled:opacity-50 ${
              isHighRisk
                ? 'bg-red-600 hover:bg-red-700'
                : 'bg-amber-600 hover:bg-amber-700'
            }`}
          >
            {isConfirming ? 'Applying...' : 'Yes, Apply Changes'}
          </button>
        </div>
      </div>
    </div>
  )
}
