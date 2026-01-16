import { Bot } from 'lucide-react'

export default function ThinkingIndicator() {
  return (
    <div className="flex gap-3">
      {/* Avatar */}
      <div className="flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center bg-gradient-to-br from-emerald-400 to-cyan-500">
        <Bot className="w-4 h-4 text-white" />
      </div>

      {/* Thinking bubble */}
      <div className="flex-1 max-w-[85%]">
        <div className="inline-flex items-center gap-1 px-4 py-3 bg-gray-100 rounded-2xl rounded-bl-md">
          <div className="flex gap-1">
            <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
            <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
            <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
          </div>
        </div>
        <div className="text-xs text-gray-400 mt-1">Thinking...</div>
      </div>
    </div>
  )
}
