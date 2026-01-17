import { useState } from 'react';
import {
  ModularLoader,
  AIThinkingLoader,
  AIGeneratingLoader,
  DataUploadLoader,
  TransformLoader,
  DeployLoader,
  SuccessLoader,
  ErrorLoader,
  SavingLoader,
  type LoaderAnimation,
  type LoaderColor,
  type LoaderSize,
  type LoaderPreset,
} from '../components/ModularLoader';

const ANIMATIONS: LoaderAnimation[] = ['pulseGlow', 'dataFlow', 'breathingScale', 'bounceElastic'];
const COLORS: LoaderColor[] = ['purple', 'blue', 'teal', 'green', 'orange', 'red', 'gray'];
const SIZES: LoaderSize[] = ['xs', 'sm', 'md', 'lg', 'xl'];
const PRESETS: LoaderPreset[] = [
  'appLaunch', 'pageNavigation', 'dataUpload',
  'aiThinking', 'aiGenerating', 'aiExecuting', 'aiComplete',
  'transformRunning', 'transformComplete',
  'moduleDeploying', 'moduleRunning',
  'saving', 'success', 'error'
];

const PRESET_DESCRIPTIONS: Record<LoaderPreset, string> = {
  appLaunch: 'Initial app load / splash screen',
  pageNavigation: 'Navigating between pages',
  dataUpload: 'Uploading CSV data',
  aiThinking: 'AI processing user message',
  aiGenerating: 'AI creating transformation plan',
  aiExecuting: 'AI executing code',
  aiComplete: 'AI task completed (single pulse)',
  transformRunning: 'Transformation in progress',
  transformComplete: 'Transformation done',
  moduleDeploying: 'Deploying module',
  moduleRunning: 'Module running on new data',
  saving: 'Saving / syncing data',
  success: 'Success confirmation',
  error: 'Error state',
};

export function LoaderDemoPage() {
  const [selectedAnimation, setSelectedAnimation] = useState<LoaderAnimation>('pulseGlow');
  const [selectedColor, setSelectedColor] = useState<LoaderColor>('purple');
  const [selectedSize, setSelectedSize] = useState<LoaderSize>('md');
  const [speed, setSpeed] = useState(1);

  return (
    <div className="min-h-screen bg-[#0a0a0f] text-white p-8">
      <div className="max-w-6xl mx-auto">
        {/* Header */}
        <header className="text-center mb-12">
          <h1 className="text-4xl font-bold mb-2 bg-gradient-to-r from-[#4B25D3] to-[#7c5ce7] bg-clip-text text-transparent">
            ModularLoader Component
          </h1>
          <p className="text-gray-400">
            Animated loading indicators for ModularData
          </p>
        </header>

        {/* Interactive Playground */}
        <section className="bg-[#111118] rounded-2xl p-8 mb-12 border border-[#1a1a24]">
          <h2 className="text-xl font-semibold mb-6">Interactive Playground</h2>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
            {/* Preview */}
            <div className="flex items-center justify-center bg-[#0a0a0f] rounded-xl p-12 min-h-[300px]">
              <ModularLoader
                animation={selectedAnimation}
                color={selectedColor}
                size={selectedSize}
                speed={speed}
              />
            </div>

            {/* Controls */}
            <div className="space-y-6">
              {/* Animation */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-2">Animation</label>
                <div className="flex flex-wrap gap-2">
                  {ANIMATIONS.map((anim) => (
                    <button
                      key={anim}
                      onClick={() => setSelectedAnimation(anim)}
                      className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                        selectedAnimation === anim
                          ? 'bg-[#4B25D3] text-white'
                          : 'bg-[#1a1a24] text-gray-400 hover:bg-[#252530]'
                      }`}
                    >
                      {anim}
                    </button>
                  ))}
                </div>
              </div>

              {/* Color */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-2">Color</label>
                <div className="flex flex-wrap gap-2">
                  {COLORS.map((c) => (
                    <button
                      key={c}
                      onClick={() => setSelectedColor(c)}
                      className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                        selectedColor === c
                          ? 'bg-[#4B25D3] text-white'
                          : 'bg-[#1a1a24] text-gray-400 hover:bg-[#252530]'
                      }`}
                    >
                      {c}
                    </button>
                  ))}
                </div>
              </div>

              {/* Size */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-2">Size</label>
                <div className="flex flex-wrap gap-2">
                  {SIZES.map((s) => (
                    <button
                      key={s}
                      onClick={() => setSelectedSize(s)}
                      className={`px-3 py-1.5 rounded-lg text-sm transition-colors ${
                        selectedSize === s
                          ? 'bg-[#4B25D3] text-white'
                          : 'bg-[#1a1a24] text-gray-400 hover:bg-[#252530]'
                      }`}
                    >
                      {s}
                    </button>
                  ))}
                </div>
              </div>

              {/* Speed */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-2">
                  Speed: {speed.toFixed(1)}x
                </label>
                <input
                  type="range"
                  min="0.3"
                  max="2"
                  step="0.1"
                  value={speed}
                  onChange={(e) => setSpeed(parseFloat(e.target.value))}
                  className="w-full accent-[#4B25D3]"
                />
              </div>

              {/* Code */}
              <div>
                <label className="block text-sm font-medium text-gray-400 mb-2">Usage</label>
                <pre className="bg-[#0a0a0f] rounded-lg p-4 text-sm text-gray-300 overflow-x-auto">
{`<ModularLoader
  animation="${selectedAnimation}"
  color="${selectedColor}"
  size="${selectedSize}"
  speed={${speed}}
/>`}
                </pre>
              </div>
            </div>
          </div>
        </section>

        {/* Presets Grid */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6">Context Presets</h2>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
            {PRESETS.map((preset) => (
              <div
                key={preset}
                className="bg-[#111118] rounded-xl p-6 border border-[#1a1a24] flex flex-col items-center"
              >
                <ModularLoader preset={preset} size="md" />
                <h3 className="mt-4 text-sm font-medium text-white">{preset}</h3>
                <p className="mt-1 text-xs text-gray-500 text-center">
                  {PRESET_DESCRIPTIONS[preset]}
                </p>
              </div>
            ))}
          </div>
        </section>

        {/* Convenience Components */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6">Convenience Components</h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {[
              { name: 'AIThinkingLoader', component: <AIThinkingLoader size="sm" /> },
              { name: 'AIGeneratingLoader', component: <AIGeneratingLoader size="sm" /> },
              { name: 'DataUploadLoader', component: <DataUploadLoader size="sm" /> },
              { name: 'TransformLoader', component: <TransformLoader size="sm" /> },
              { name: 'DeployLoader', component: <DeployLoader size="sm" /> },
              { name: 'SuccessLoader', component: <SuccessLoader size="sm" /> },
              { name: 'ErrorLoader', component: <ErrorLoader size="sm" /> },
              { name: 'SavingLoader', component: <SavingLoader size="sm" /> },
            ].map(({ name, component }) => (
              <div
                key={name}
                className="bg-[#111118] rounded-xl p-4 border border-[#1a1a24] flex flex-col items-center"
              >
                {component}
                <code className="mt-3 text-xs text-gray-400">{`<${name} />`}</code>
              </div>
            ))}
          </div>
        </section>

        {/* All Animations x Colors Matrix */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6">Animation × Color Matrix</h2>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-[#1a1a24]">
                  <th className="py-3 px-4 text-left text-sm font-medium text-gray-400">Animation</th>
                  {COLORS.map((c) => (
                    <th key={c} className="py-3 px-4 text-center text-sm font-medium text-gray-400 capitalize">
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {ANIMATIONS.map((anim) => (
                  <tr key={anim} className="border-b border-[#1a1a24]">
                    <td className="py-4 px-4 text-sm text-gray-300">{anim}</td>
                    {COLORS.map((c) => (
                      <td key={c} className="py-4 px-4">
                        <div className="flex justify-center">
                          <ModularLoader animation={anim} color={c} size="sm" />
                        </div>
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>

        {/* Size Comparison */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-6">Size Comparison</h2>
          <div className="bg-[#111118] rounded-xl p-8 border border-[#1a1a24]">
            <div className="flex items-end justify-center gap-8">
              {SIZES.map((s) => (
                <div key={s} className="flex flex-col items-center">
                  <ModularLoader animation="pulseGlow" size={s} />
                  <span className="mt-4 text-sm text-gray-400">{s}</span>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Usage Examples */}
        <section>
          <h2 className="text-xl font-semibold mb-6">Usage Examples</h2>
          <div className="bg-[#111118] rounded-xl p-6 border border-[#1a1a24]">
            <pre className="text-sm text-gray-300 overflow-x-auto">
{`// Basic usage
import { ModularLoader } from '@/components/ModularLoader';

<ModularLoader animation="pulseGlow" color="purple" size="md" />

// Using presets
<ModularLoader preset="aiThinking" />
<ModularLoader preset="moduleDeploying" />
<ModularLoader preset="success" loop={false} onAnimationComplete={() => setDone(true)} />

// Convenience components
import { AIThinkingLoader, TransformLoader, SuccessLoader } from '@/components/ModularLoader';

<AIThinkingLoader />
<TransformLoader size="lg" />
<SuccessLoader onAnimationComplete={handleSuccess} />

// Custom speed
<ModularLoader preset="aiGenerating" speed={1.5} />

// With hook for state management
import { useLoaderState } from '@/components/ModularLoader';

const loader = useLoaderState('aiThinking');

// Show loader
loader.show();

// Change state
loader.setPreset('aiExecuting');

// Transition with fade
await loader.transition('success', 300);

// Hide loader
loader.hide();`}
            </pre>
          </div>
        </section>

        {/* Footer */}
        <footer className="mt-12 text-center text-gray-500 text-sm">
          ModularData Loader Components
        </footer>
      </div>
    </div>
  );
}

export default LoaderDemoPage;
