import { Handle, Position } from '@xyflow/react'
import { FileSpreadsheet, Wand2 } from 'lucide-react'

interface DataNodeProps {
  data: {
    label: string
    rowCount: number
    colCount: number
    isSelected: boolean
    isCurrent: boolean
  }
  selected: boolean
}

export default function DataNode({ data, selected }: DataNodeProps) {
  const isOriginal = data.label === 'Original Data'

  return (
    <div
      className={`
        bg-white rounded-xl shadow-sm border-2 px-4 py-3 min-w-[160px]
        transition-all duration-150
        ${selected ? 'border-blue-500 shadow-md' : 'border-gray-200'}
        ${data.isCurrent ? 'ring-2 ring-blue-300 ring-offset-2' : ''}
      `}
    >
      {/* Input Handle */}
      {!isOriginal && (
        <Handle
          type="target"
          position={Position.Left}
          className="w-3 h-3 !bg-gray-400 border-2 border-white"
        />
      )}

      {/* Node Content */}
      <div className="flex items-start gap-3">
        <div
          className={`
            w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0
            ${isOriginal ? 'bg-emerald-100' : 'bg-blue-100'}
          `}
        >
          {isOriginal ? (
            <FileSpreadsheet className={`w-5 h-5 ${isOriginal ? 'text-emerald-600' : 'text-blue-600'}`} />
          ) : (
            <Wand2 className="w-5 h-5 text-blue-600" />
          )}
        </div>
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-medium text-gray-900 truncate">
            {data.label}
          </h3>
          <p className="text-xs text-gray-500 mt-0.5">
            {data.rowCount.toLocaleString()} rows x {data.colCount} cols
          </p>
        </div>
      </div>

      {/* Output Handle */}
      <Handle
        type="source"
        position={Position.Right}
        className="w-3 h-3 !bg-blue-500 border-2 border-white"
      />
    </div>
  )
}
