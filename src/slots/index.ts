import React from 'react';
import { CadastralMapClickHandler } from '../components/CadastralMapClickHandler';
import { CadastralClickToggle } from '../components/CadastralClickToggle';
import { GeoSearchBar } from '../components/GeoSearchBar';
import { CadastralBuildingLayer } from '../components/CadastralBuildingLayer';
import { CadastralBuildingsToggle } from '../components/CadastralBuildingsToggle';
import { CadastralProvider } from '../context/CadastralContext';
import type { ModuleViewerSlots, SlotWidgetDefinition } from '@nekazari/sdk';

// Module identifier - used for all slot widgets
const MODULE_ID = 'catastro-spain';

/**
 * Catastro Spain Module Slots Configuration
 * 
 * All widgets include explicit moduleId for proper host integration.
 * This module adds:
 * - Click-to-add functionality for cadastral parcels on the /entities page
 * - Geocoding search bar to navigate to locations by name or postal code
 */
export const moduleSlots: ModuleViewerSlots = {
  'map-layer': [
    {
      id: 'catastro-spain-click-handler',
      moduleId: MODULE_ID,
      component: 'CadastralMapClickHandler',
      priority: 100,
      localComponent: CadastralMapClickHandler,
    },
    {
      id: 'catastro-spain-geo-search',
      moduleId: MODULE_ID,
      component: 'GeoSearchBar',
      priority: 50,
      localComponent: GeoSearchBar,
    },
    {
      id: 'catastro-spain-building-layer',
      moduleId: MODULE_ID,
      component: 'CadastralBuildingLayer',
      priority: 90,
      localComponent: CadastralBuildingLayer,
    },
  ],
  'layer-toggle': [
    {
      id: 'catastro-spain-click-toggle',
      moduleId: MODULE_ID,
      component: 'CadastralClickToggle',
      priority: 20,
      localComponent: CadastralClickToggle,
    },
    {
      id: 'catastro-spain-buildings-toggle',
      moduleId: MODULE_ID,
      component: 'CadastralBuildingsToggle',
      priority: 15,
      localComponent: CadastralBuildingsToggle,
    },
  ],
  'context-panel': [],
  'bottom-panel': [],
  'entity-tree': [],

  // Provider for sharing state between components
  moduleProvider: CadastralProvider,
};

/**
 * Export as viewerSlots for host integration
 */
export const viewerSlots = moduleSlots;

// Also export as default for Module Federation compatibility
export default viewerSlots;
