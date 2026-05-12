"""Single source of truth for org-level resource limits.

Business model is prepaid credits ($0.10/DTE, never expire). There are
no per-plan caps on companies, DTEs/month, users, sucursales, API keys,
or webhooks. These constants exist only as large defaults for the
legacy `organizations.monthly_quota` and `organizations.max_companies`
columns when admin endpoints populate them.

Enforcement reads `organizations.max_companies` from the DB:
  NULL, 0, or >= UNLIMITED_MAX_COMPANIES → no cap.
Anything below acts as a manual override for a specific org (rare).
"""

UNLIMITED_DTE_QUOTA = 999999
UNLIMITED_MAX_COMPANIES = 9999


def is_unlimited_companies(max_companies: int | None) -> bool:
    """Return True when the org has no company cap.

    NULL or 0 → unlimited (column unset). >= UNLIMITED_MAX_COMPANIES → also
    unlimited (column set to the canonical 9999+ sentinel).
    """
    if max_companies is None:
        return True
    if max_companies <= 0:
        return True
    return max_companies >= UNLIMITED_MAX_COMPANIES
