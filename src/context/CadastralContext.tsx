import React, { createContext, useContext, useState, useEffect, ReactNode } from 'react';

/** What a map click selects: SIGPAC enclosures (default) or the cadastral parcel. */
export type ClickMode = 'sigpac' | 'cadastral';

interface CadastralContextType {
  isClickEnabled: boolean;
  clickMode: ClickMode;
  setClickMode: (mode: ClickMode) => void;
  toggleClickEnabled: () => void;
  setClickEnabled: (enabled: boolean) => void;
}

// Global state storage key
const GLOBAL_STATE_KEY = '__nekazari_cadastral_state__';

// Global state management (shared across all provider instances)
const getGlobalState = (): boolean => {
  if (typeof window === 'undefined') return false;
  const stored = (window as any)[GLOBAL_STATE_KEY];
  return stored !== undefined ? stored : false;
};

const setGlobalState = (value: boolean) => {
  if (typeof window === 'undefined') return;
  (window as any)[GLOBAL_STATE_KEY] = value;
  // Dispatch custom event to notify all provider instances
  window.dispatchEvent(new CustomEvent('cadastral-state-changed', { detail: value }));
};

const GLOBAL_MODE_KEY = '__nekazari_cadastral_mode__';

const getGlobalMode = (): ClickMode => {
  if (typeof window === 'undefined') return 'sigpac';
  return (window as any)[GLOBAL_MODE_KEY] === 'cadastral' ? 'cadastral' : 'sigpac';
};

const setGlobalMode = (mode: ClickMode) => {
  if (typeof window === 'undefined') return;
  (window as any)[GLOBAL_MODE_KEY] = mode;
  window.dispatchEvent(new CustomEvent('cadastral-mode-changed', { detail: mode }));
};

const CadastralContext = createContext<CadastralContextType | undefined>(undefined);

export const CadastralProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  // Initialize from global state
  const [isClickEnabled, setIsClickEnabled] = useState(() => getGlobalState());
  const [clickMode, setClickModeState] = useState<ClickMode>(() => getGlobalMode());

  useEffect(() => {
    const handleModeChange = (event: CustomEvent) => setClickModeState(event.detail);
    window.addEventListener('cadastral-mode-changed', handleModeChange as EventListener);
    return () => window.removeEventListener('cadastral-mode-changed', handleModeChange as EventListener);
  }, []);

  const setClickMode = (mode: ClickMode) => {
    setClickModeState(mode);
    setGlobalMode(mode);
  };

  // Listen for global state changes from other provider instances
  useEffect(() => {
    const handleStateChange = (event: CustomEvent) => {
      setIsClickEnabled(event.detail);
    };

    window.addEventListener('cadastral-state-changed', handleStateChange as EventListener);
    return () => {
      window.removeEventListener('cadastral-state-changed', handleStateChange as EventListener);
    };
  }, []);

  const toggleClickEnabled = () => {
    const newValue = !isClickEnabled;
    setIsClickEnabled(newValue);
    setGlobalState(newValue);
  };

  const setClickEnabled = (enabled: boolean) => {
    setIsClickEnabled(enabled);
    setGlobalState(enabled);
  };

  return (
    <CadastralContext.Provider
      value={{
        isClickEnabled,
        clickMode,
        setClickMode,
        toggleClickEnabled,
        setClickEnabled,
      }}
    >
      {children}
    </CadastralContext.Provider>
  );
};

export const useCadastral = (): CadastralContextType => {
  const context = useContext(CadastralContext);
  if (!context) {
    throw new Error('useCadastral must be used within a CadastralProvider');
  }
  return context;
};

