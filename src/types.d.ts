import type { ModuleViewerSlots } from '@nekazari/sdk';

declare global {
    interface Window {
        __NKZ__: {
            register: (registration: {
                id: string;
                viewerSlots?: ModuleViewerSlots;
                provider?: any;
                version?: string;
            }) => void;
        };
        // Shared deps exposed by host
        React: typeof import('react');
        ReactDOM: typeof import('react-dom');
        ReactRouterDOM: typeof import('react-router-dom');
        __NKZ_SDK__: typeof import('@nekazari/sdk');
        __NKZ_UI__: typeof import('@nekazari/ui-kit');
        __NKZ_DESIGN_TOKENS__: typeof import('@nekazari/design-tokens');
        __NKZ_VIEWER_KIT__: typeof import('@nekazari/viewer-kit');
    }
}

export { };
