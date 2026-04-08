import { i18n } from '@nekazari/sdk';
import en from './locales/en.json';
import es from './locales/es.json';

const CADASTRAL_NAMESPACE = 'cadastral';

export function registerCadastralTranslations(): void {
  if (!i18n || typeof (i18n as any).addResourceBundle !== 'function') return;
  i18n.addResourceBundle('en', CADASTRAL_NAMESPACE, en, true, true);
  i18n.addResourceBundle('es', CADASTRAL_NAMESPACE, es, true, true);
}

registerCadastralTranslations();

