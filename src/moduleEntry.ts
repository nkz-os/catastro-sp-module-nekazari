import { defineModule } from "@nekazari/module-kit";
import App from './App'; // Use the full App component
import { moduleSlots } from './slots/index';
import pkg from '../package.json';

// Use strict module ID that matches database
const MODULE_ID = 'catastro-spain';

console.log(`[${MODULE_ID}] Initializing module v${pkg.version}`);


// Self-register with the host runtime
if (window.__NKZ__) {
    // @ts-ignore - 'main' property is supported by host runtime but not yet in SDK type definition
    window.__NKZ__.register({
        id: MODULE_ID,
        viewerSlots: moduleSlots,
        main: App, // Register the App component
        version: pkg.version,
    });
} else {
    console.error(`[${MODULE_ID}] window.__NKZ__ not found! Module registration failed.`);
}
