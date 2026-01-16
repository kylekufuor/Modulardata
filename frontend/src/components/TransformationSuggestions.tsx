import { Sparkles, Trash2, Type, Calendar, Phone, Hash, X } from 'lucide-react'

interface ColumnProfile {
  name: string
  dtype: string
  semantic_type: string
  null_count: number
  null_percent: number
  unique_count?: number
  issues?: string[]
}

interface TransformationSuggestionsProps {
  profile: {
    row_count: number
    column_count: number
    columns: ColumnProfile[]
  }
  onSuggestionClick: (message: string) => void
  onDismiss?: () => void
}

interface Suggestion {
  icon: typeof Sparkles
  label: string
  message: string
  priority: number
}

export default function TransformationSuggestions({
  profile,
  onSuggestionClick,
  onDismiss,
}: TransformationSuggestionsProps) {
  const suggestions = generateSuggestions(profile)

  if (suggestions.length === 0) {
    return null
  }

  // Take top 3 suggestions
  const topSuggestions = suggestions.slice(0, 3)

  return (
    <div className="px-2 py-3 bg-gradient-to-r from-blue-50 to-indigo-50 rounded-xl border border-blue-100">
      <div className="flex items-center justify-between mb-2 px-1">
        <span className="text-xs font-medium text-blue-700 flex items-center gap-1">
          <Sparkles className="w-3 h-3" />
          Suggested actions
        </span>
        {onDismiss && (
          <button
            onClick={onDismiss}
            className="p-0.5 hover:bg-blue-100 rounded text-blue-400 hover:text-blue-600"
          >
            <X className="w-3 h-3" />
          </button>
        )}
      </div>
      <div className="flex flex-wrap gap-2">
        {topSuggestions.map((suggestion, idx) => (
          <button
            key={idx}
            onClick={() => onSuggestionClick(suggestion.message)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-white hover:bg-gray-50 text-gray-700 text-sm rounded-lg border border-gray-200 transition-colors shadow-sm"
          >
            <suggestion.icon className="w-3.5 h-3.5 text-gray-500" />
            {suggestion.label}
          </button>
        ))}
      </div>
    </div>
  )
}

function generateSuggestions(profile: TransformationSuggestionsProps['profile']): Suggestion[] {
  const suggestions: Suggestion[] = []
  const { columns } = profile

  // Check for columns with missing values
  const columnsWithNulls = columns.filter(col => col.null_count > 0)
    .sort((a, b) => b.null_count - a.null_count)

  if (columnsWithNulls.length > 0) {
    const topNullCol = columnsWithNulls[0]
    if (columnsWithNulls.length === 1) {
      suggestions.push({
        icon: Trash2,
        label: `Fill missing ${topNullCol.name}`,
        message: `fill missing values in ${topNullCol.name}`,
        priority: 10,
      })
    } else {
      suggestions.push({
        icon: Trash2,
        label: `Clean missing values`,
        message: `remove rows with missing values`,
        priority: 10,
      })
    }
  }

  // Check for potential name columns (needs standardization)
  const nameColumns = columns.filter(col =>
    col.name.toLowerCase().includes('name') ||
    col.name.toLowerCase().includes('first') ||
    col.name.toLowerCase().includes('last')
  )
  if (nameColumns.length > 0) {
    suggestions.push({
      icon: Type,
      label: `Standardize names`,
      message: `standardize the names (trim whitespace and fix casing)`,
      priority: 8,
    })
  }

  // Check for date columns
  const dateColumns = columns.filter(col =>
    col.semantic_type?.toLowerCase().includes('date') ||
    col.name.toLowerCase().includes('date') ||
    col.name.toLowerCase().includes('created') ||
    col.name.toLowerCase().includes('timestamp')
  )
  if (dateColumns.length > 0) {
    suggestions.push({
      icon: Calendar,
      label: `Standardize dates`,
      message: `standardize date formats to YYYY-MM-DD`,
      priority: 7,
    })
  }

  // Check for phone columns
  const phoneColumns = columns.filter(col =>
    col.semantic_type?.toLowerCase().includes('phone') ||
    col.name.toLowerCase().includes('phone') ||
    col.name.toLowerCase().includes('mobile') ||
    col.name.toLowerCase().includes('tel')
  )
  if (phoneColumns.length > 0) {
    suggestions.push({
      icon: Phone,
      label: `Format phone numbers`,
      message: `standardize phone number formats`,
      priority: 6,
    })
  }

  // Check for potential duplicate detection
  const emailColumns = columns.filter(col =>
    col.semantic_type?.toLowerCase().includes('email') ||
    col.name.toLowerCase().includes('email')
  )
  const idColumns = columns.filter(col =>
    col.name.toLowerCase().includes('id') ||
    col.name.toLowerCase() === 'id'
  )
  if (emailColumns.length > 0 || idColumns.length > 0) {
    const dedupeCol = emailColumns[0]?.name || idColumns[0]?.name
    suggestions.push({
      icon: Hash,
      label: `Remove duplicates`,
      message: `remove duplicate rows${dedupeCol ? ` based on ${dedupeCol}` : ''}`,
      priority: 5,
    })
  }

  // Sort by priority (highest first)
  return suggestions.sort((a, b) => b.priority - a.priority)
}
