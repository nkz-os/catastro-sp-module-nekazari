import { i18n } from '@nekazari/sdk';
import en from './locales/en.json';
import es from './locales/es.json';
import eu from './locales/eu.json';
import fr from './locales/fr.json';
import pt from './locales/pt.json';
import ca from './locales/ca.json';

const CADASTRAL_NAMESPACE = 'cadastral';

export function registerCadastralTranslations(): void {
  if (!i18n || typeof (i18n as any).addResourceBundle !== 'function') return;
  i18n.addResourceBundle('en', CADASTRAL_NAMESPACE, en, true, true);
  i18n.addResourceBundle('es', CADASTRAL_NAMESPACE, es, true, true);
  i18n.addResourceBundle('eu', CADASTRAL_NAMESPACE, eu, true, true);
  i18n.addResourceBundle('fr', CADASTRAL_NAMESPACE, fr, true, true);
  i18n.addResourceBundle('pt', CADASTRAL_NAMESPACE, pt, true, true);
  i18n.addResourceBundle('ca', CADASTRAL_NAMESPACE, ca, true, true);
}

registerCadastralTranslations();

