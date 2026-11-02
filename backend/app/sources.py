"""Registry of external data sources used by the module, with licence citations."""

FEGA_SIGPAC = 'fega_sigpac'

SOURCES = {
    FEGA_SIGPAC: {
        'id': FEGA_SIGPAC,
        'name': 'SIGPAC - FEGA',
        'publisher': 'Fondo Espanol de Garantia Agraria (FEGA)',
        'url': 'https://sigpac-hubcloud.es/',
        'license': 'CC BY 4.0',
        'license_url': 'https://creativecommons.org/licenses/by/4.0/',
        'attribution': 'Source: SIGPAC, FEGA (CC BY 4.0)',
    },
}


def get_source(source_id: str) -> dict:
    return dict(SOURCES[source_id])
