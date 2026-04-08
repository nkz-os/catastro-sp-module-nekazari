import React from 'react';
import { useAuth, useTranslation } from '@nekazari/sdk';
import { Card } from '@nekazari/ui-kit';
import { MapPin, CheckCircle } from 'lucide-react';
import './index.css';
import './i18n';

const CatastroSpainApp: React.FC = () => {
  const { user, tenantId, isAuthenticated } = useAuth();
  const { t } = useTranslation('cadastral');

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-green-50 p-6">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-3 mb-4">
            <div className="p-3 bg-blue-100 rounded-lg">
              <MapPin className="w-8 h-8 text-blue-600" />
            </div>
            <h1 className="text-4xl font-bold text-gray-900">
              {t('app.title')}
            </h1>
          </div>
          <p className="text-gray-600 text-lg">
            {t('app.subtitle')}
          </p>
        </div>

        {/* Content Card */}
        <Card padding="lg" className="mb-6">
          <div className="space-y-4">
            <div className="flex items-start gap-3">
              <CheckCircle className="w-6 h-6 text-green-500 flex-shrink-0 mt-0.5" />
              <div>
                <h2 className="text-xl font-semibold text-gray-900 mb-2">
                  {t('info.activeTitle')}
                </h2>
                <p className="text-gray-600">
                  {t('info.activeDesc')}
                </p>
              </div>
            </div>

            <div className="pt-4 border-t border-gray-200">
              <h3 className="font-semibold text-gray-900 mb-2">{t('info.featuresTitle')}:</h3>
              <ul className="list-disc list-inside space-y-1 text-gray-600">
                <li>{t('info.features.byCoords')}</li>
                <li>{t('info.features.oneClick')}</li>
                <li>{t('info.features.regions')}</li>
                <li>{t('info.features.syncOrion')}</li>
              </ul>
            </div>

            {isAuthenticated && (
              <div className="pt-4 border-t border-gray-200">
                <p className="text-sm text-gray-500">
                  {t('info.authenticatedAs')}: <span className="font-medium">{user?.email || user?.name}</span>
                  {tenantId && <span className="ml-2">({t('info.tenant')}: {tenantId})</span>}
                </p>
              </div>
            )}
          </div>
        </Card>
      </div>
    </div>
  );
};

// CRITICAL: Export as default - required for Module Federation
export default CatastroSpainApp;


