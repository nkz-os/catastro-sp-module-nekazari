import { useSyncExternalStore } from 'react';

/**
 * Visibility of the 3D buildings layer, shared by the layer-toggle and the
 * map-layer widgets. Module-level so it survives the toggle being unmounted
 * whenever the host closes its Layers panel.
 */
let visible = false;
const listeners = new Set<() => void>();

export const buildingsLayerStore = {
  getVisible: (): boolean => visible,
  setVisible: (next: boolean): void => {
    if (next === visible) return;
    visible = next;
    listeners.forEach((l) => l());
  },
  subscribe: (listener: () => void): (() => void) => {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
};

export function useBuildingsLayerVisible(): [boolean, (next: boolean) => void] {
  const value = useSyncExternalStore(buildingsLayerStore.subscribe, buildingsLayerStore.getVisible);
  return [value, buildingsLayerStore.setVisible];
}
