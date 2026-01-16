import React, { useState } from 'react'
import { Bot, User, ThumbsUp, ThumbsDown, Send, X } from 'lucide-react'
import type { ChatMessage as ChatMessageType } from '../types'
import { api } from '../lib/api'

interface ChatMessageProps {
  message: ChatMessageType
  sessionId?: string
  showFeedback?: boolean
}

// Simple markdown-like parser for chat messages
function formatMessage(content: string): React.ReactNode[] {
  const lines = content.split('\n')
  const elements: React.ReactNode[] = []
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
function formatInlineText(text: string): React.ReactNode[] {
  const parts: React.ReactNode[] = []
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

export default function ChatMessage({ message, sessionId, showFeedback = false }: ChatMessageProps) {
  const isUser = message.role === 'user'
  const [feedbackGiven, setFeedbackGiven] = useState<'positive' | 'negative' | null>(null)
  const [showCommentInput, setShowCommentInput] = useState(false)
  const [comment, setComment] = useState('')
  const [submitting, setSubmitting] = useState(false)

  // Check if this message indicates a completed transformation
  const isTransformationResult = !isUser && (
    message.content.startsWith('Done!') ||
    message.content.includes('Applied') ||
    message.content.includes('transformation')
  )

  const shouldShowFeedback = showFeedback && isTransformationResult && sessionId && !feedbackGiven

  const handleFeedback = async (rating: 'positive' | 'negative') => {
    if (!sessionId) return

    if (rating === 'negative') {
      setShowCommentInput(true)
      setFeedbackGiven(rating)
      return
    }

    // Positive feedback - submit immediately
    try {
      setSubmitting(true)
      await api.submitFeedback({
        session_id: sessionId,
        message_id: message.id,
        rating,
        node_id: message.node_id,
      })
      setFeedbackGiven(rating)
    } catch (err) {
      console.error('Failed to submit feedback:', err)
    } finally {
      setSubmitting(false)
    }
  }

  const handleCommentSubmit = async () => {
    if (!sessionId) return

    try {
      setSubmitting(true)
      await api.submitFeedback({
        session_id: sessionId,
        message_id: message.id,
        rating: 'negative',
        comment: comment.trim() || undefined,
        node_id: message.node_id,
      })
      setShowCommentInput(false)
    } catch (err) {
      console.error('Failed to submit feedback:', err)
    } finally {
      setSubmitting(false)
    }
  }

  const handleSkipComment = async () => {
    if (!sessionId) return

    try {
      setSubmitting(true)
      await api.submitFeedback({
        session_id: sessionId,
        message_id: message.id,
        rating: 'negative',
        node_id: message.node_id,
      })
      setShowCommentInput(false)
    } catch (err) {
      console.error('Failed to submit feedback:', err)
    } finally {
      setSubmitting(false)
    }
  }

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

        {/* Feedback buttons */}
        {shouldShowFeedback && (
          <div className="flex items-center gap-2 mt-2">
            <span className="text-xs text-gray-400">Was this helpful?</span>
            <button
              onClick={() => handleFeedback('positive')}
              disabled={submitting}
              className="p-1.5 hover:bg-green-50 rounded-full text-gray-400 hover:text-green-600 transition-colors disabled:opacity-50"
              title="Yes, this was helpful"
            >
              <ThumbsUp className="w-4 h-4" />
            </button>
            <button
              onClick={() => handleFeedback('negative')}
              disabled={submitting}
              className="p-1.5 hover:bg-red-50 rounded-full text-gray-400 hover:text-red-600 transition-colors disabled:opacity-50"
              title="No, this wasn't right"
            >
              <ThumbsDown className="w-4 h-4" />
            </button>
          </div>
        )}

        {/* Feedback given indicator */}
        {feedbackGiven && !showCommentInput && (
          <div className="flex items-center gap-1.5 mt-2 text-xs text-gray-400">
            {feedbackGiven === 'positive' ? (
              <>
                <ThumbsUp className="w-3.5 h-3.5 text-green-500" />
                <span>Thanks for your feedback!</span>
              </>
            ) : (
              <>
                <ThumbsDown className="w-3.5 h-3.5 text-red-500" />
                <span>Thanks for letting us know</span>
              </>
            )}
          </div>
        )}

        {/* Comment input for negative feedback */}
        {showCommentInput && (
          <div className="mt-2 p-3 bg-gray-50 rounded-lg border border-gray-200">
            <p className="text-xs text-gray-600 mb-2">What went wrong? (optional)</p>
            <div className="flex gap-2">
              <input
                type="text"
                value={comment}
                onChange={(e) => setComment(e.target.value)}
                placeholder="e.g., It filtered the wrong column..."
                className="flex-1 text-sm px-3 py-1.5 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    handleCommentSubmit()
                  }
                }}
              />
              <button
                onClick={handleCommentSubmit}
                disabled={submitting}
                className="px-3 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
              >
                <Send className="w-4 h-4" />
              </button>
              <button
                onClick={handleSkipComment}
                disabled={submitting}
                className="px-2 py-1.5 text-gray-500 hover:text-gray-700 text-sm"
                title="Skip"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>
        )}

        <div className={`text-xs text-gray-400 mt-1 ${isUser ? 'text-right' : ''}`}>
          {formatTime(message.created_at)}
        </div>
      </div>
    </div>
  )
}
