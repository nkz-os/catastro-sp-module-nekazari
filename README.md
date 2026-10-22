# Catastro Spain Module for Nekazari

Spanish Cadastre integration module for the Nekazari agricultural platform.

## Features

- **Multi-region support**: Spain (State), Navarra, Euskadi
- **Reverse geocoding**: Query parcels by coordinates
- **Click-to-add parcels**: Add cadastral parcels with a single click on the map
- **Orion-LD sync**: Webhook receiver for AgriParcel entity synchronization
- **PostGIS cache**: Spatial queries for agricultural analytics

## Frontend Features

### Click-to-Add Parcels

The module adds a click handler to the `/entities` page that allows users to add cadastral parcels with a single click:

1. Navigate to the Entities page (`/entities`)
2. Click on any empty space on the map
3. The module will:
   - Query the cadastral service for the clicked coordinates
   - If a parcel is found, automatically create it in Orion-LD
   - Show a success notification with parcel details
   - Reload the page to display the new parcel

**Note**: The click handler only activates when:
- You're on the `/entities` page
- You click on empty space (not on an existing entity)
- The cadastral service returns geometry for the parcel

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/parcels` | GET | List tenant parcels |
| `/parcels` | POST | Create parcel |
| `/parcels/<id>` | GET | Get parcel |
| `/parcels/<id>` | PUT | Update parcel |
| `/parcels/<id>` | DELETE | Delete parcel |
| `/parcels/summary` | GET | Tenant statistics |
| `/parcels/query-by-coordinates` | POST | Reverse geocode from coordinates |
| `/orion/notify` | POST | Webhook for Orion-LD notifications |

## Requirements

- PostgreSQL with PostGIS extension
- Access to Orion-LD (for sync)
- Keycloak (for authentication)
- Node.js 18+ (for frontend build)

## Environment Variables

```bash
POSTGRES_HOST=postgresql-service
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret
POSTGRES_DB=nekazari
ORION_URL=http://orion-ld:1026
KEYCLOAK_URL=http://keycloak:8080
```

## Development

### Frontend

```bash
cd nkz-module-cadastrial_sp
npm install
npm run dev
```

### Backend

```bash
cd backend
pip install -r requirements.txt
python app/cadastral_api.py
```

## Building

### Frontend

```bash
npm run build
```

The build output will be in the `dist/` directory, including `remoteEntry.js` for Module Federation.

### Backend

```bash
docker build -t catastro-module-backend ./backend
```

## Deployment

Deploy with the manifests in `k8s/` (`backend-deployment.yaml`, `frontend-deployment.yaml`).

## Module Integration

This module integrates with the Nekazari platform through:

1. **Slot System**: Registers a `map-layer` slot component that handles map clicks
2. **Viewer Context**: Uses the `useViewer` hook to access the Cesium viewer instance
3. **API Integration**: Uses the platform's cadastral and parcel APIs

## License

AGPL-3.0
