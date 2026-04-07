"""
authorizer.py — Dual-mode authorization (session + stateless API token).

The Authorizer works identically regardless of how the user authenticated.
Routes never need to know which auth path was used.

Session (browser):
    - tenant_id and roles are read from the Flask session (zero DB queries)
    - The session is populated at login and refreshed on tenant switch

Token (API):
    - No session exists. tenant_id and roles are unknown at construction time.
    - On first call to assert_tenant() or a resource method with tenant_id,
      the Authorizer resolves membership + roles with a single DB query
    - Result is cached for the lifetime of the request (on the Authorizer instance)

Usage in routes (unchanged):

    auth = Authorizer(current_user)                    # works for both paths
    auth.assert_tenant(tenant_id, role="viewer")       # session: string check, token: 1 query
    flow = auth.flow(flow_id, role="editor", tenant_id=tenant_id)
"""

from flask import session, abort, g
from app.utils.decorators import set_session_data, AUTH_SESSION, AUTH_TOKEN

# ── Role hierarchy ────────────────────────────────────────────────────────────

ROLE_LEVELS = {"user": 0, "viewer": 1, "editor": 2, "admin": 3}


def _role_level(role: str) -> int:
    return ROLE_LEVELS.get(role, -1)


def role_gte(role: str, minimum: str) -> bool:
    """True if role meets or exceeds minimum."""
    min_lvl = _role_level(minimum)
    if min_lvl == -1:
        return False
    return _role_level(role) >= min_lvl


def roles_gte_any(roles: list, minimum: str) -> bool:
    """True if any role in the list meets or exceeds minimum."""
    return any(role_gte(r, minimum) for r in roles)


# ── Model registry ────────────────────────────────────────────────────────────

_m = {}


def init_authorizer(models: dict):
    """Register SQLAlchemy model classes. Call once at app startup."""
    _m.update(models)


def _require_init():
    if not _m:
        raise RuntimeError(
            "Authorizer not initialized — call init_authorizer() at app startup"
        )


# ── Tenant creation check ────────────────────────────────────────────────────

def _can_create_tenant(user) -> bool:
    if user.super:
        return True
    return (
        user.can_user_create_tenant
        and len(user.get_tenants(own=True)) < user.tenant_limit
    )


# ── Authorizer ────────────────────────────────────────────────────────────────

class Authorizer:
    __slots__ = ("user", "tenant_id", "roles", "_tenant", "_is_super",
                 "_auth_mode", "_tenant_resolved")

    def __init__(self, user):
        _require_init()

        if not user.is_authenticated or not user.is_active:
            abort(401, description="Not authenticated")

        self.user = user
        self._is_super = bool(user.super)
        self._tenant = None
        self._tenant_resolved = False

        # Detect auth mode from flask.g (set by login_required decorator)
        self._auth_mode = getattr(g, "auth_mode", AUTH_SESSION)

        if self._auth_mode == AUTH_SESSION:
            self._init_from_session()
        else:
            self._init_from_token()

    # ── Initialization paths ──────────────────────────────────────────────

    def _init_from_session(self):
        """Browser path: read tenant context from session. Zero queries."""
        self.tenant_id = session.get("tenant_id")
        self.roles = session.get("tenant_roles", [])

        if self.user.session_stale:
            self._refresh_stale_session()

    def _init_from_token(self):
        """API token path: no tenant context yet.
        Will be resolved lazily on first tenant-scoped operation."""
        self.tenant_id = None
        self.roles = []

    def _refresh_stale_session(self):
        """Re-sync session roles if an admin changed them since last request."""
        from app.models import db
        tenant = _m["Tenant"].query.get(self.tenant_id)
        if tenant:
            set_session_data(self.user, tenant)
        self.user.session_stale = False
        db.session.commit()
        self.roles = session.get("tenant_roles", [])

    # ── Tenant resolution (API token path) ────────────────────────────────

    def _resolve_tenant(self, tenant_id: str):
        """Resolve the user's membership and roles for a tenant.
        Called once per request on the API token path. Single DB query.

        For session path, this is never called — roles come from session.
        """
        if self._is_super:
            self.tenant_id = tenant_id
            self.roles = list(ROLE_LEVELS.keys())  # super has all roles
            self._tenant_resolved = True
            return

        member = _m["TenantMember"].query.filter_by(
            user_id=self.user.id,
            tenant_id=tenant_id,
        ).first()

        if not member:
            abort(403, description="Not a member of this tenant")

        self.tenant_id = tenant_id
        # member.roles should be a list like ["viewer", "editor"]
        # Adapt this to however your TenantMember model stores roles
        if hasattr(member, 'roles') and isinstance(member.roles, list):
            self.roles = member.roles
        elif hasattr(member, 'role'):
            self.roles = [member.role] if member.role else []
        else:
            self.roles = []
        self._tenant_resolved = True

    def _ensure_tenant_context(self, tenant_id: str):
        """Ensure we have tenant context, resolving if needed (API path).
        For session path, validates the URL tenant_id matches session.

        This is the single choke point for all tenant-scoped operations.
        """
        if self._auth_mode == AUTH_TOKEN:
            # API path: resolve on first use, validate consistency on subsequent
            if not self._tenant_resolved:
                self._resolve_tenant(tenant_id)
            elif tenant_id != self.tenant_id:
                # Same request is trying to access two different tenants
                abort(403, description="Cannot access multiple tenants in a single request")
        else:
            # Session path: URL must match session tenant
            if not self._is_super and tenant_id != self.tenant_id:
                abort(403, description="Tenant mismatch")

    # ── Properties ────────────────────────────────────────────────────────

    @property
    def is_super(self) -> bool:
        return self._is_super

    # ── Role checks ───────────────────────────────────────────────────────

    def has_role(self, minimum: str) -> bool:
        if self._is_super:
            return True
        return roles_gte_any(self.roles, minimum)

    def assert_role(self, minimum: str) -> "Authorizer":
        if not self.has_role(minimum):
            abort(403, description=f"Requires '{minimum}' role or above")
        return self

    def assert_super(self) -> "Authorizer":
        if not self._is_super:
            abort(403, description="Superadmin required")
        return self

    # ── Tenant assertions ─────────────────────────────────────────────────

    def assert_tenant(self, tenant_id: str, *, role: str = "viewer") -> "Authorizer":
        """Verify the user can access this tenant with the required role.

        Session path: string comparison (zero queries).
        Token path: resolves membership + roles (one query, cached).
        """
        self._ensure_tenant_context(tenant_id)
        self.assert_role(role)
        return self

    def switch_tenant(self, tenant_id: str):
        """Verify the user can switch to the given tenant.
        Only meaningful for session-based auth."""
        t = _m["Tenant"].query.get(tenant_id)
        if not t:
            abort(404, description="Tenant not found")
        if not self._is_super and not t.has_member(self.user):
            abort(403, description="You are not a member of this tenant")
        return t

    # ── Credential helpers ────────────────────────────────────────────────

    def is_credential_owner_or_admin(self, credential) -> bool:
        if self._is_super:
            return True
        if credential.created_by == self.user.id:
            return True
        return self.has_role("admin")

    def assert_credential_owner_or_admin(self, credential) -> "Authorizer":
        if not self.is_credential_owner_or_admin(credential):
            abort(403, description="Only the credential owner or an admin can do this")
        return self

    # ── Generic resource fetch ────────────────────────────────────────────

    def _fetch(self, model_key: str, resource_id: str, *, role: str,
               tenant_id: str | None = None, label: str = "Resource"):
        """Fetch a tenant-scoped resource with authorization.

        1. Validate/resolve tenant context (string check or 1 query)
        2. Assert role (zero queries)
        3. Fetch resource scoped to tenant (1 query)
        """
        if tenant_id is not None:
            self._ensure_tenant_context(tenant_id)
        self.assert_role(role)
        obj = _m[model_key].query.filter_by(
            id=resource_id,
            tenant_id=self.tenant_id,
        ).first()
        if not obj:
            abort(404, description=f"{label} not found")
        return obj

    # ── Resource methods ──────────────────────────────────────────────────

    def flow(self, flow_id: str, *, role: str, tenant_id: str | None = None):
        return self._fetch("Flow", flow_id, role=role, tenant_id=tenant_id, label="Flow")

    def dashboard(self, dashboard_id: str, *, role: str, tenant_id: str | None = None):
        return self._fetch("Dashboard", dashboard_id, role=role, tenant_id=tenant_id, label="Dashboard")

    def ticket(self, ticket_id: str, *, role: str, tenant_id: str | None = None):
        return self._fetch("Ticket", ticket_id, role=role, tenant_id=tenant_id, label="Ticket")

    def record(self, record_id: str, *, role: str, tenant_id: str | None = None):
        return self._fetch("DataRecord", record_id, role=role, tenant_id=tenant_id, label="Record")

    def data_table(self, table_id: str, *, role: str, tenant_id: str | None = None):
        return self._fetch("DataTable", table_id, role=role, tenant_id=tenant_id, label="DataTable")

    def credential(self, credential_id: str, *, role: str, tenant_id: str | None = None):
        """Credential has special logic: role check happens after fetch for super users."""
        if tenant_id is not None:
            self._ensure_tenant_context(tenant_id)
        c = _m["Credential"].query.filter_by(
            id=credential_id,
            tenant_id=self.tenant_id,
        ).first()
        if not c:
            abort(404, description="Credential not found")
        if self._is_super:
            return c
        self.assert_role(role)
        return c

    def run(self, run_id: str, *, role: str, tenant_id: str | None = None):
        """Run requires a join to verify tenant ownership through the flow."""
        if tenant_id is not None:
            self._ensure_tenant_context(tenant_id)
        self.assert_role(role)
        r = (
            _m["Run"].query
            .join(_m["Flow"], _m["Flow"].id == _m["Run"].flow_id)
            .filter(
                _m["Run"].id == run_id,
                _m["Flow"].tenant_id == self.tenant_id,
            )
            .first()
        )
        if not r:
            abort(404, description="Run not found")
        return r

    # ── Non-tenant-scoped resources ───────────────────────────────────────

    def tenant(self):
        """Fetch the current tenant object."""
        if not self.tenant_id:
            abort(403, description="No tenant in context")
        if self._tenant:
            return self._tenant
        self._tenant = _m["Tenant"].query.get(self.tenant_id)
        if not self._tenant:
            abort(404, description="Tenant not found")
        return self._tenant

    def own_user(self, user_id: str):
        """Fetch a user record. Only self or super can access."""
        if not self._is_super and self.user.id != user_id:
            abort(403, description="Cannot access another user's data")
        if self.user.id == user_id:
            return self.user
        u = _m["User"].query.get(user_id)
        if not u:
            abort(404, description="User not found")
        return u

    def tenant_user(self, user_id: str, *, role: str = "admin"):
        """Fetch a user who is a member of the current tenant."""
        self.assert_role(role)
        member = _m["TenantMember"].query.filter_by(
            user_id=user_id,
            tenant_id=self.tenant_id,
        ).first()
        if not member:
            abort(404, description="User not found in tenant")
        return member.user