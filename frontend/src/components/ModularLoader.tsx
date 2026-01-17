import React, { useEffect, useId, useMemo, useState } from 'react';

// ============================================================================
// TYPES
// ============================================================================

export type LoaderAnimation = 'pulseGlow' | 'dataFlow' | 'breathingScale' | 'bounceElastic';

export type LoaderColor =
  | 'purple'    // Brand default (#4B25D3)
  | 'blue'      // Processing (#3B82F6)
  | 'teal'      // Success/Running (#10B981)
  | 'green'     // Complete (#22C55E)
  | 'orange'    // Warning/Deploying (#F59E0B)
  | 'red'       // Error (#EF4444)
  | 'gray';     // Background/Saving (#6B7280)

export type LoaderSize = 'xs' | 'sm' | 'md' | 'lg' | 'xl';

export type LoaderPreset =
  | 'appLaunch'           // Pulse Glow, Purple
  | 'pageNavigation'      // Breathing Scale, Purple
  | 'dataUpload'          // Data Flow, Blue
  | 'aiThinking'          // Bounce Elastic, Purple (slow)
  | 'aiGenerating'        // Bounce Elastic, Purple (fast)
  | 'aiExecuting'         // Data Flow, Blue
  | 'aiComplete'          // Pulse Glow, Green (single pulse)
  | 'transformRunning'    // Data Flow, Purple
  | 'transformComplete'   // Pulse Glow, Green
  | 'moduleDeploying'     // Pulse Glow, Orange
  | 'moduleRunning'       // Data Flow, Teal
  | 'saving'              // Breathing Scale, Gray
  | 'success'             // Pulse Glow, Green
  | 'error';              // Pulse Glow, Red

export interface ModularLoaderProps {
  /** Animation style */
  animation?: LoaderAnimation;
  /** Color theme */
  color?: LoaderColor;
  /** Size of the loader */
  size?: LoaderSize;
  /** Use a preset configuration */
  preset?: LoaderPreset;
  /** Animation speed multiplier (0.5 = half speed, 2 = double speed) */
  speed?: number;
  /** Whether the animation should loop */
  loop?: boolean;
  /** Callback when animation completes one cycle (useful for single-shot animations) */
  onAnimationComplete?: () => void;
  /** Optional label for accessibility */
  label?: string;
  /** Additional CSS classes */
  className?: string;
}

// ============================================================================
// CONSTANTS
// ============================================================================

const COLOR_MAP: Record<LoaderColor, { primary: string; glow: string; light: string }> = {
  purple: { primary: '#4B25D3', glow: 'rgba(75, 37, 211, 0.8)', light: '#9b7aff' },
  blue:   { primary: '#3B82F6', glow: 'rgba(59, 130, 246, 0.8)', light: '#93c5fd' },
  teal:   { primary: '#10B981', glow: 'rgba(16, 185, 129, 0.8)', light: '#6ee7b7' },
  green:  { primary: '#22C55E', glow: 'rgba(34, 197, 94, 0.8)', light: '#86efac' },
  orange: { primary: '#F59E0B', glow: 'rgba(245, 158, 11, 0.8)', light: '#fcd34d' },
  red:    { primary: '#EF4444', glow: 'rgba(239, 68, 68, 0.8)', light: '#fca5a5' },
  gray:   { primary: '#6B7280', glow: 'rgba(107, 114, 128, 0.6)', light: '#9ca3af' },
};

const SIZE_MAP: Record<LoaderSize, number> = {
  xs: 24,
  sm: 40,
  md: 64,
  lg: 96,
  xl: 128,
};

const PRESET_CONFIG: Record<LoaderPreset, { animation: LoaderAnimation; color: LoaderColor; speed: number; loop: boolean }> = {
  appLaunch:         { animation: 'pulseGlow', color: 'purple', speed: 1, loop: true },
  pageNavigation:    { animation: 'breathingScale', color: 'purple', speed: 1, loop: true },
  dataUpload:        { animation: 'dataFlow', color: 'blue', speed: 1, loop: true },
  aiThinking:        { animation: 'bounceElastic', color: 'purple', speed: 0.7, loop: true },
  aiGenerating:      { animation: 'bounceElastic', color: 'purple', speed: 1.3, loop: true },
  aiExecuting:       { animation: 'dataFlow', color: 'blue', speed: 1.2, loop: true },
  aiComplete:        { animation: 'pulseGlow', color: 'green', speed: 1, loop: false },
  transformRunning:  { animation: 'dataFlow', color: 'purple', speed: 1, loop: true },
  transformComplete: { animation: 'pulseGlow', color: 'green', speed: 1, loop: false },
  moduleDeploying:   { animation: 'pulseGlow', color: 'orange', speed: 0.8, loop: true },
  moduleRunning:     { animation: 'dataFlow', color: 'teal', speed: 1, loop: true },
  saving:            { animation: 'breathingScale', color: 'gray', speed: 1.2, loop: true },
  success:           { animation: 'pulseGlow', color: 'green', speed: 1, loop: false },
  error:             { animation: 'pulseGlow', color: 'red', speed: 0.8, loop: true },
};

// ============================================================================
// LOGO PATH (ModularData M shape)
// ============================================================================

const LOGO_PATH = "M312.748 236.872C312.748 242.395 317.225 246.872 322.748 246.872H420.369C425.892 246.872 430.369 251.349 430.369 256.872V362C430.369 367.523 425.892 372 420.369 372H315.241C309.718 372 305.241 367.523 305.241 362V263.128C305.241 257.605 300.764 253.128 295.241 253.128H205.128C199.605 253.128 195.128 257.605 195.128 263.128V362C195.128 367.523 190.651 372 185.128 372H80C74.4772 372 70 367.523 70 362V256.872C70 251.349 74.4772 246.872 80 246.872H177.62C183.143 246.872 187.62 242.395 187.62 236.872V138C187.62 132.477 192.097 128 197.62 128H302.748C308.271 128 312.748 132.477 312.748 138V236.872Z";

const VIEWBOX = "70 120 360 260";

// ============================================================================
// COMPONENT
// ============================================================================

export function ModularLoader({
  animation: animationProp,
  color: colorProp,
  size = 'md',
  preset,
  speed: speedProp,
  loop: loopProp,
  onAnimationComplete,
  label = 'Loading',
  className = '',
}: ModularLoaderProps) {
  const uniqueId = useId();

  // Resolve props from preset or direct values
  const config = preset ? PRESET_CONFIG[preset] : null;
  const animation = animationProp ?? config?.animation ?? 'pulseGlow';
  const color = colorProp ?? config?.color ?? 'purple';
  const speed = speedProp ?? config?.speed ?? 1;
  const loop = loopProp ?? config?.loop ?? true;

  const colors = COLOR_MAP[color];
  const sizeValue = SIZE_MAP[size];

  // Generate unique animation names to avoid conflicts
  const animationName = useMemo(() => `modular-${animation}-${uniqueId.replace(/:/g, '')}`, [animation, uniqueId]);

  // Handle animation end for non-looping animations
  useEffect(() => {
    if (!loop && onAnimationComplete) {
      const duration = getAnimationDuration(animation) / speed;
      const timer = setTimeout(onAnimationComplete, duration * 1000);
      return () => clearTimeout(timer);
    }
  }, [loop, onAnimationComplete, animation, speed]);

  return (
    <div
      className={`inline-flex items-center justify-center ${className}`}
      style={{ width: sizeValue, height: sizeValue }}
      role="status"
      aria-label={label}
    >
      <style>{generateKeyframes(animationName, animation, colors, speed, loop)}</style>
      {renderAnimation(animation, animationName, colors, uniqueId)}
    </div>
  );
}

// ============================================================================
// ANIMATION RENDERERS
// ============================================================================

function getAnimationDuration(animation: LoaderAnimation): number {
  switch (animation) {
    case 'pulseGlow': return 2;
    case 'dataFlow': return 1.5;
    case 'breathingScale': return 3;
    case 'bounceElastic': return 1.5;
  }
}

function generateKeyframes(
  name: string,
  animation: LoaderAnimation,
  colors: typeof COLOR_MAP[LoaderColor],
  speed: number,
  loop: boolean
): string {
  const duration = getAnimationDuration(animation) / speed;
  const iteration = loop ? 'infinite' : '1';

  switch (animation) {
    case 'pulseGlow':
      return `
        @keyframes ${name} {
          0%, 100% {
            filter: drop-shadow(0 0 8px ${colors.glow.replace('0.8', '0.3')});
            transform: scale(1);
          }
          50% {
            filter: drop-shadow(0 0 25px ${colors.glow});
            transform: scale(1.02);
          }
        }
        .${name} {
          animation: ${name} ${duration}s ease-in-out ${iteration};
        }
      `;

    case 'dataFlow':
      return `
        @keyframes ${name}-flow {
          0% { transform: translateX(-100%); }
          100% { transform: translateX(100%); }
        }
        .${name}-flow {
          animation: ${name}-flow ${duration}s linear ${iteration};
        }
      `;

    case 'breathingScale':
      return `
        @keyframes ${name} {
          0%, 100% {
            transform: scale(0.95);
            opacity: 0.8;
          }
          50% {
            transform: scale(1.05);
            opacity: 1;
          }
        }
        .${name} {
          animation: ${name} ${duration}s ease-in-out ${iteration};
        }
      `;

    case 'bounceElastic':
      return `
        @keyframes ${name} {
          0%, 100% {
            transform: translateY(0) scale(1);
          }
          30% {
            transform: translateY(-15%) scale(1.05, 0.95);
          }
          50% {
            transform: translateY(0) scale(0.95, 1.05);
          }
          70% {
            transform: translateY(-5%) scale(1.02);
          }
        }
        .${name} {
          animation: ${name} ${duration}s ease-in-out ${iteration};
        }
      `;
  }
}

function renderAnimation(
  animation: LoaderAnimation,
  animationName: string,
  colors: typeof COLOR_MAP[LoaderColor],
  uniqueId: string
): React.ReactElement {
  const svgProps = {
    viewBox: VIEWBOX,
    fill: "none",
    xmlns: "http://www.w3.org/2000/svg",
    style: { width: '100%', height: '100%' },
  };

  switch (animation) {
    case 'pulseGlow':
      return (
        <svg {...svgProps} className={animationName}>
          <path d={LOGO_PATH} fill={colors.primary} />
        </svg>
      );

    case 'dataFlow':
      const gradientId = `flow-grad-${uniqueId.replace(/:/g, '')}`;
      const clipId = `logo-clip-${uniqueId.replace(/:/g, '')}`;
      return (
        <svg {...svgProps} style={{ ...svgProps.style, overflow: 'hidden' }}>
          <defs>
            <linearGradient id={gradientId} x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" stopColor={colors.primary} />
              <stop offset="40%" stopColor={colors.primary} />
              <stop offset="50%" stopColor={colors.light} />
              <stop offset="60%" stopColor={colors.primary} />
              <stop offset="100%" stopColor={colors.primary} />
            </linearGradient>
            <clipPath id={clipId}>
              <path d={LOGO_PATH} />
            </clipPath>
          </defs>
          <path d={LOGO_PATH} fill={colors.primary} />
          <g clipPath={`url(#${clipId})`}>
            <rect
              className={`${animationName}-flow`}
              x="0"
              y="120"
              width="500"
              height="260"
              fill={`url(#${gradientId})`}
              opacity="0.6"
            />
          </g>
        </svg>
      );

    case 'breathingScale':
      return (
        <svg {...svgProps} className={animationName}>
          <path d={LOGO_PATH} fill={colors.primary} />
        </svg>
      );

    case 'bounceElastic':
      return (
        <svg {...svgProps} className={animationName}>
          <path d={LOGO_PATH} fill={colors.primary} />
        </svg>
      );
  }
}

// ============================================================================
// CONVENIENCE COMPONENTS
// ============================================================================

/** Pre-configured loader for AI chat thinking state */
export function AIThinkingLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="aiThinking" {...props} />;
}

/** Pre-configured loader for AI generating/planning state */
export function AIGeneratingLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="aiGenerating" {...props} />;
}

/** Pre-configured loader for data upload */
export function DataUploadLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="dataUpload" {...props} />;
}

/** Pre-configured loader for transformation running */
export function TransformLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="transformRunning" {...props} />;
}

/** Pre-configured loader for module deployment */
export function DeployLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="moduleDeploying" {...props} />;
}

/** Pre-configured loader for success flash */
export function SuccessLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="success" {...props} />;
}

/** Pre-configured loader for error state */
export function ErrorLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="error" {...props} />;
}

/** Pre-configured loader for saving/syncing */
export function SavingLoader(props: Omit<ModularLoaderProps, 'preset'>) {
  return <ModularLoader preset="saving" {...props} />;
}

// ============================================================================
// HOOK FOR PROGRAMMATIC CONTROL
// ============================================================================

export interface LoaderState {
  preset: LoaderPreset;
  visible: boolean;
}

export function useLoaderState(initialPreset: LoaderPreset = 'appLaunch') {
  const [state, setState] = useState<LoaderState>({ preset: initialPreset, visible: false });

  return {
    ...state,
    show: (preset?: LoaderPreset) => setState({ preset: preset ?? state.preset, visible: true }),
    hide: () => setState(s => ({ ...s, visible: false })),
    setPreset: (preset: LoaderPreset) => setState(s => ({ ...s, preset })),
    transition: async (toPreset: LoaderPreset, delayMs = 300) => {
      setState(s => ({ ...s, visible: false }));
      await new Promise(r => setTimeout(r, delayMs));
      setState({ preset: toPreset, visible: true });
    },
  };
}

export default ModularLoader;
