import { AIThinkingLoader } from './ModularLoader'

export default function ThinkingIndicator() {
  return (
    <div className="flex gap-3">
      {/* Logo as avatar */}
      <div className="flex-shrink-0 w-8 h-8 flex items-center justify-center">
        <AIThinkingLoader size="xs" />
      </div>

      {/* Thinking bubble */}
      <div className="flex-1 max-w-[85%]">
        <div className="inline-flex items-center gap-2 px-4 py-3 bg-[#111118] border border-[#1a1a24] rounded-2xl rounded-bl-md">
          <AIThinkingLoader size="sm" />
          <span className="text-sm text-gray-400">Thinking...</span>
        </div>
      </div>
    </div>
  )
}
