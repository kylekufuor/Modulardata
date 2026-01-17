export default function ThinkingIndicator() {
  return (
    <div className="flex gap-3">
      {/* Logo avatar */}
      <div className="flex-shrink-0 w-8 h-8 rounded-full bg-[#4B25D3] flex items-center justify-center">
        <svg width="16" height="16" viewBox="70 128 360 244" fill="none">
          <path d="M312.748 236.872C312.748 242.395 317.225 246.872 322.748 246.872H420.369C425.892 246.872 430.369 251.349 430.369 256.872V362C430.369 367.523 425.892 372 420.369 372H315.241C309.718 372 305.241 367.523 305.241 362V263.128C305.241 257.605 300.764 253.128 295.241 253.128H205.128C199.605 253.128 195.128 257.605 195.128 263.128V362C195.128 367.523 190.651 372 185.128 372H80C74.4772 372 70 367.523 70 362V256.872C70 251.349 74.4772 246.872 80 246.872H177.62C183.143 246.872 187.62 242.395 187.62 236.872V138C187.62 132.477 192.097 128 197.62 128H302.748C308.271 128 312.748 132.477 312.748 138V236.872Z" fill="white"/>
        </svg>
      </div>

      {/* Thinking bubble - clean and minimal */}
      <div className="flex-1 max-w-[85%]">
        <div className="inline-flex items-center gap-1.5 px-4 py-3 bg-gray-100 rounded-2xl rounded-bl-md">
          <span className="w-2 h-2 bg-[#4B25D3] rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
          <span className="w-2 h-2 bg-[#4B25D3] rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
          <span className="w-2 h-2 bg-[#4B25D3] rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
        </div>
        <div className="text-xs text-gray-400 mt-1">Thinking...</div>
      </div>
    </div>
  )
}
