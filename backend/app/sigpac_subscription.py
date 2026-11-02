"""Orion subscription: re-run the SIGPAC lookup when an AgriParcel geometry changes.

Disabled unless ``SIGPAC_NOTIFY_URL`` (the receiver's in-cluster URL) is set.
Receiver auth uses derived notification tokens (``notify_auth``).
"""

import asyncio
import logging
import os
import threading

logger = logging.getLogger(__name__)

MODULE_NAME = 'catastro-spain'
HEAL_INTERVAL_MIN = 60


def build_registrar():
    from nkz_platform_sdk import SubscriptionRegistrar
    from nkz_platform_sdk.notify_auth import derived_tokens_from_env
    return SubscriptionRegistrar(
        orion_url=os.getenv('ORION_URL', 'http://orion-ld-service:1026'),
        notification_url=os.environ['SIGPAC_NOTIFY_URL'],
        subscriptions=[{'type': 'AgriParcel', 'throttling': 5,
                        'watched_attributes': ['location']}],
        module_name=MODULE_NAME,
        context_url=os.getenv('CONTEXT_URL') or None,
        internal_secret=os.getenv('INTERNAL_SERVICE_SECRET', ''),
        derived_tokens=derived_tokens_from_env(),
    )


async def _enabled_tenants() -> list[str]:
    from nkz_platform_sdk.installations import installed_tenants
    return list((await installed_tenants(MODULE_NAME)).enabled)


async def _run(registrar) -> None:
    try:
        await registrar.ensure_all(await _enabled_tenants())
    except Exception as exc:
        logger.error('sigpac.subscription initial ensure failed: %s', exc)
    await registrar.periodic_heal(_enabled_tenants, interval_minutes=HEAL_INTERVAL_MIN)


def start_background() -> bool:
    if not os.getenv('SIGPAC_NOTIFY_URL'):
        return False
    try:
        registrar = build_registrar()
    except Exception as exc:
        logger.error('sigpac.subscription disabled, bad configuration: %s', exc)
        return False
    threading.Thread(target=lambda: asyncio.run(_run(registrar)),
                     name='sigpac-subscription', daemon=True).start()
    return True
