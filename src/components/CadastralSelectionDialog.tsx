import React from 'react';
import { X, MapPin, Building, LandPlot, ArrowRight, Layers } from 'lucide-react';
import { useTranslation } from '@nekazari/sdk';
import { CadastralData } from '../services/cadastralApi';

interface CadastralSelectionDialogProps {
    candidates: CadastralData[];
    onSelect: (candidate: CadastralData) => void;
    onCancel: () => void;
}

export const CadastralSelectionDialog: React.FC<CadastralSelectionDialogProps> = ({
    candidates,
    onSelect,
    onCancel,
}) => {
    const { t } = useTranslation('cadastral');

    // Helper to determine icon based on type or content
    const getIcon = (candidate: CadastralData) => {
        const type = candidate.type?.toLowerCase() || '';
        if (type.includes('parcel') || type.includes('finca') || type.includes('predio')) {
            return <LandPlot className="w-5 h-5 text-green-600" />;
        }
        if (type.includes('edif') || type.includes('const')) {
            return <Building className="w-5 h-5 text-gray-600" />;
        }
        return <Layers className="w-5 h-5 text-blue-600" />;
    };

    // Helper to format type name
    const formatType = (candidate: CadastralData) => {
        if (!candidate.type) return t('dialogs.cadastralEntity');
        // Remove "IDENA:" prefix if present and format
        const clean = candidate.type.replace(/^.*:/, '').replace(/_/g, ' ');
        return clean.charAt(0).toUpperCase() + clean.slice(1);
    };

    return (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-50 p-4 pointer-events-auto">
            <div className="bg-white rounded-xl shadow-2xl max-w-lg w-full overflow-hidden flex flex-col max-h-[80vh]">
                {/* Header */}
                <div className="bg-gradient-to-r from-blue-600 to-blue-700 px-6 py-4 flex items-center justify-between flex-shrink-0">
                    <div className="flex items-center gap-3">
                        <Layers className="w-5 h-5 text-white" />
                        <div>
                            <h3 className="text-lg font-semibold text-white">{t('dialogs.multipleTitle')}</h3>
                            <p className="text-xs text-blue-100">{t('dialogs.multipleSubtitle')}</p>
                        </div>
                    </div>
                    <button
                        onClick={onCancel}
                        className="text-white hover:text-blue-100 transition-colors"
                        aria-label={t('dialogs.close')}
                    >
                        <X className="w-5 h-5" />
                    </button>
                </div>

                {/* List */}
                <div className="overflow-y-auto p-2 space-y-2 bg-gray-50 flex-grow">
                    {candidates.map((candidate, index) => (
                        <button
                            key={index}
                            onClick={() => onSelect(candidate)}
                            className="w-full text-left bg-white p-4 rounded-lg border border-gray-200 shadow-sm hover:shadow-md hover:border-blue-300 hover:ring-1 hover:ring-blue-300 transition-all group"
                        >
                            <div className="flex items-start gap-3">
                                <div className="p-2 bg-gray-50 rounded-full group-hover:bg-blue-50 transition-colors">
                                    {getIcon(candidate)}
                                </div>

                                <div className="flex-1 min-w-0">
                                    <div className="flex items-center justify-between mb-1">
                                        <span className="text-xs font-semibold uppercase tracking-wide text-gray-500 bg-gray-100 px-2 py-0.5 rounded-full">
                                            {formatType(candidate)}
                                        </span>
                                        {candidate.region && (
                                            <span className="text-[10px] text-gray-400 capitalize border border-gray-100 px-1 rounded">
                                                {candidate.region}
                                            </span>
                                        )}
                                    </div>

                                    <p className="text-sm font-mono font-bold text-gray-900 truncate mb-1">
                                        {candidate.cadastralReference}
                                    </p>

                                    <div className="flex items-center gap-1 text-xs text-gray-600 truncate">
                                        <MapPin className="w-3 h-3 flex-shrink-0" />
                                        <span className="truncate">
                                            {candidate.address || candidate.municipality || t('dialogs.noAddress')}
                                        </span>
                                    </div>
                                </div>

                                <div className="flex items-center self-center text-gray-300 group-hover:text-blue-500 transition-colors">
                                    <ArrowRight className="w-5 h-5" />
                                </div>
                            </div>
                        </button>
                    ))}
                </div>

                {/* Footer */}
                <div className="px-6 py-3 bg-gray-50 border-t border-gray-200 text-xs text-center text-gray-500 flex-shrink-0">
                    {t('dialogs.foundCount', { count: candidates.length })}
                </div>
            </div>
        </div>
    );
};
