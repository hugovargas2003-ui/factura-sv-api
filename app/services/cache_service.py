"""
FACTURA-SV: Cache Service (Upstash Redis)
==========================================
Caches MH auth tokens (valid 24h) to reduce API calls.
Falls back to no-op if UPSTASH_REDIS_URL is not set.

Usage:
    from app.services.cache_service import cache_mh_token, get_cached_mh_token

    cached = get_cached_mh_token(org_id)
    if cached:
        return TokenInfo.from_cache(cached)

    token = await authenticate_with_mh(...)
    cache_mh_token(org_id, token.to_dict())
"""
import json
import logging
import os

logger = logging.getLogger("factura-sv")

REDIS_URL = os.getenv("UPSTASH_REDIS_URL")

_redis_client = None


def _get_redis():
    """Lazy-init Redis client. Returns None if not configured."""
    global _redis_client
    if not REDIS_URL:
        return None
    if _redis_client is None:
        try:
            import redis
            _redis_client = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
            _redis_client.ping()
            logger.info("Redis cache connected (Upstash)")
        except Exception as e:
            logger.warning(f"Redis not available, cache disabled: {e}")
            _redis_client = None
            return None
    return _redis_client


def cache_mh_token(org_id: str, token_data: dict, ttl: int = 82800) -> None:
    """Cache MH auth token for 23 hours (token is valid 24h, 1h safety margin)."""
    r = _get_redis()
    if not r:
        return
    try:
        r.setex(f"mh_token:{org_id}", ttl, json.dumps(token_data))
        logger.debug(f"Cached MH token for org {org_id[:8]}...")
    except Exception as e:
        logger.warning(f"Failed to cache MH token: {e}")


def get_cached_mh_token(org_id: str) -> dict | None:
    """Get cached MH auth token if still valid."""
    r = _get_redis()
    if not r:
        return None
    try:
        data = r.get(f"mh_token:{org_id}")
        if data:
            logger.debug(f"Cache hit: MH token for org {org_id[:8]}...")
            return json.loads(data)
    except Exception as e:
        logger.warning(f"Failed to read MH token cache: {e}")
    return None


def invalidate_mh_token(org_id: str) -> None:
    """Remove cached MH token (e.g. on auth failure)."""
    r = _get_redis()
    if not r:
        return
    try:
        r.delete(f"mh_token:{org_id}")
    except Exception:
        pass


def cache_get(key: str) -> str | None:
    """Generic cache get."""
    r = _get_redis()
    if not r:
        return None
    try:
        return r.get(key)
    except Exception:
        return None


def cache_set(key: str, value: str, ttl: int = 3600) -> None:
    """Generic cache set with TTL."""
    r = _get_redis()
    if not r:
        return
    try:
        r.setex(key, ttl, value)
    except Exception:
        pass
