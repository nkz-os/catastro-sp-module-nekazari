import React, { useRef, useState } from 'react';
import { X, MapPin, CheckCircle, XCircle } from 'lucide-react';
import { useTranslation } from '@nekazari/sdk';
import { CadastralData } from '../services/cadastralApi';

interface CadastralConfirmDialogProps {
  cadastralData: CadastralData;
  area: number;
  onConfirm: () => void;
  onCancel: () => void;
  isProcessing?: boolean;
}

export const CadastralConfirmDialog: React.FC<CadastralConfirmDialogProps> = ({
  cadastralData,
  area,
  onConfirm,
  onCancel,
  isProcessing = false,
}) => {
  const { t } = useTranslation('cadastral');
  const [position, setPosition] = useState({ x: 24, y: 24 });
  const dragRef = useRef<{ startX: number; startY: number; originX: number; originY: number; active: boolean }>({
    startX: 0,
    startY: 0,
    originX: 0,
    originY: 0,
    active: false,
  });

  const beginDrag = (clientX: number, clientY: number) => {
    dragRef.current = {
      startX: clientX,
      startY: clientY,
      originX: position.x,
      originY: position.y,
      active: true,
    };
  };

  const updateDrag = (clientX: number, clientY: number) => {
    if (!dragRef.current.active) return;
    const nextX = Math.max(8, dragRef.current.originX + (clientX - dragRef.current.startX));
    const nextY = Math.max(8, dragRef.current.originY + (clientY - dragRef.current.startY));
    setPosition({ x: nextX, y: nextY });
  };

  const endDrag = () => {
    dragRef.current.active = false;
  };

  return (
    <div className="fixed inset-0 z-50 pointer-events-none">
      <div
        className="pointer-events-auto bg-white rounded-xl shadow-2xl max-w-md w-[calc(100%-2rem)] overflow-hidden"
        style={{ position: 'absolute', left: `${position.x}px`, top: `${position.y}px` }}
      >
        {/* Header */}
        <div
          className="bg-gradient-to-r from-blue-500 to-blue-600 px-6 py-4 flex items-center justify-between cursor-move select-none"
          onMouseDown={(e) => beginDrag(e.clientX, e.clientY)}
          onMouseMove={(e) => updateDrag(e.clientX, e.clientY)}
          onMouseUp={endDrag}
          onMouseLeave={endDrag}
          onTouchStart={(e) => beginDrag(e.touches[0].clientX, e.touches[0].clientY)}
          onTouchMove={(e) => updateDrag(e.touches[0].clientX, e.touches[0].clientY)}
          onTouchEnd={endDrag}
        >
          <div className="flex items-center gap-3">
            <MapPin className="w-5 h-5 text-white" />
            <h3 className="text-lg font-semibold text-white">{t('dialogs.foundTitle')}</h3>
          </div>
          {!isProcessing && (
            <button
              onClick={onCancel}
              className="text-white hover:text-blue-100 transition-colors"
              aria-label={t('dialogs.close')}
            >
              <X className="w-5 h-5" />
            </button>
          )}
        </div>

        {/* Content */}
        <div className="p-6 space-y-4">
          <div className="space-y-3">
            <div className="flex items-start gap-3">
              <div className="flex-1">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">{t('dialogs.cadastralRef')}</p>
                <p className="text-sm font-mono font-semibold text-gray-900">
                  {cadastralData.cadastralReference}
                </p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4 pt-2 border-t border-gray-200">
              <div>
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">{t('dialogs.municipality')}</p>
                <p className="text-sm font-medium text-gray-900">{cadastralData.municipality}</p>
              </div>
              <div>
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">{t('dialogs.province')}</p>
                <p className="text-sm font-medium text-gray-900">{cadastralData.province}</p>
              </div>
            </div>

            {cadastralData.address && (
              <div className="pt-2 border-t border-gray-200">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">{t('dialogs.address')}</p>
                <p className="text-sm text-gray-700">{cadastralData.address}</p>
              </div>
            )}

            <div className="pt-2 border-t border-gray-200">
              <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">{t('dialogs.area')}</p>
              <p className="text-lg font-semibold text-blue-600">{area.toFixed(2)} ha</p>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex items-center justify-end gap-3">
          {!isProcessing ? (
            <>
              <button
                onClick={onCancel}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              >
                {t('dialogs.cancel')}
              </button>
              <button
                onClick={onConfirm}
                className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
              >
                <CheckCircle className="w-4 h-4" />
                {t('dialogs.addParcel')}
              </button>
            </>
          ) : (
            <div className="flex items-center gap-2 text-sm text-gray-600">
              <div className="w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full animate-spin" />
              <span>{t('dialogs.adding')}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};


