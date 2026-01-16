import { Bot, User } from 'lucide-react'
import type { ChatMessage as ChatMessageType } from '../types'

interface ChatMessageProps {
  message: ChatMessageType
}

// Simple markdown-like parser for chat messages
function formatMessage(content: string): JSX.Element[] {
  const lines = content.split('\n')
  const elements: JSX.Element[] = []
  let currentList: string[] = []
  let listKey = 0

  const flushList = () => {
    if (currentList.length > 0) {
      elements.push(
        <ul key={`list-${listKey++}`} className="space-y-1 my-2">
          {currentList.map((item, i) => (
            <li key={i} className="flex items-start gap-2">
              <span className="text-gray-400 mt-0.5">•</span>
              <span>{item}</span>
            </li>
          ))}
        </ul>
      )
      currentList = []
    }
  }

  lines.forEach((line, index) => {
    const trimmedLine = line.trim()

    // Empty line
    if (trimmedLine === '') {
      flushList()
      elements.push(<div key={`space-${index}`} className="h-2" />)
      return
    }

    // Horizontal rule (━━━ or ---)
    if (/^[━─-]{3,}$/.test(trimmedLine)) {
      flushList()
      elements.push(
        <hr key={`hr-${index}`} className="border-gray-200 my-3" />
      )
      return
    }

    // List item (• or - at start)
    if (/^[•\-\*]\s/.test(trimmedLine)) {
      currentList.push(trimmedLine.replace(/^[•\-\*]\s*/, ''))
      return
    }

    // Section header with emoji (like "📊 Your Data:")
    if (/^[📊📋⚠️✅🔴🟡🔵👋]\s/.test(trimmedLine)) {
      flushList()
      elements.push(
        <div key={`header-${index}`} className="font-medium text-gray-900 mt-2">
          {trimmedLine}
        </div>
      )
      return
    }

    // Regular paragraph
    flushList()
    elements.push(
      <p key={`p-${index}`} className="text-gray-700">
        {formatInlineText(trimmedLine)}
      </p>
    )
  })

  flushList()
  return elements
}

// Format inline text (bold, code, etc.)
function formatInlineText(text: string): (string | JSX.Element)[] {
  const parts: (string | JSX.Element)[] = []
  let remaining = text
  let keyIndex = 0

  // Match **bold** or `code`
  const regex = /(\*\*([^*]+)\*\*|`([^`]+)`)/g
  let match
  let lastIndex = 0

  while ((match = regex.exec(text)) !== null) {
    // Add text before match
    if (match.index > lastIndex) {
      parts.push(text.slice(lastIndex, match.index))
    }

    if (match[2]) {
      // Bold text
      parts.push(
        <strong key={`bold-${keyIndex++}`} className="font-semibold">
          {match[2]}
        </strong>
      )
    } else if (match[3]) {
      // Code text
      parts.push(
        <code key={`code-${keyIndex++}`} className="px-1.5 py-0.5 bg-gray-100 rounded text-sm font-mono">
          {match[3]}
        </code>
      )
    }

    lastIndex = regex.lastIndex
  }

  // Add remaining text
  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex))
  }

  return parts.length > 0 ? parts : [text]
}

function formatTime(dateString: string): string {
  const date = new Date(dateString)
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

export default function ChatMessage({ message }: ChatMessageProps) {
  const isUser = message.role === 'user'

  return (
    <div className={`flex gap-3 ${isUser ? 'flex-row-reverse' : ''}`}>
      {/* Avatar */}
      <div
        className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center ${
          isUser ? 'bg-blue-600' : 'bg-gradient-to-br from-emerald-400 to-cyan-500'
        }`}
      >
        {isUser ? (
          <User className="w-4 h-4 text-white" />
        ) : (
          <Bot className="w-4 h-4 text-white" />
        )}
      </div>

      {/* Message Content */}
      <div className={`flex-1 max-w-[85%] ${isUser ? 'text-right' : ''}`}>
        <div
          className={`inline-block text-left px-4 py-3 rounded-2xl ${
            isUser
              ? 'bg-blue-600 text-white rounded-br-md'
              : 'bg-gray-100 text-gray-900 rounded-bl-md'
          }`}
        >
          {isUser ? (
            <p className="text-sm">{message.content}</p>
          ) : (
            <div className="text-sm space-y-1">
              {formatMessage(message.content)}
            </div>
          )}
        </div>
        <div className={`text-xs text-gray-400 mt-1 ${isUser ? 'text-right' : ''}`}>
          {formatTime(message.created_at)}
        </div>
      </div>
    </div>
  )
}
