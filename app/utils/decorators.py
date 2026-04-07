from functools import wraps
from flask import current_app, request, jsonify, redirect, url_for, flash, session, abort, g
from flask_login import current_user, login_user, logout_user


# ── Auth mode constants ───────────────────────────────────────────────────────

AUTH_SESSION = "session"
AUTH_TOKEN   = "token"


# ── Session helpers (browser path only) ───────────────────────────────────────

def set_session_data(user, tenant=None):
    """Write user + tenant data into the Flask session.
    Called from custom_login, refresh_session, and switch_tenant.
    Only used for browser-based authentication."""
    session.permanent = True
    session['user_id']    = user.id
    session['user_email'] = user.email
    session['user_super'] = user.super
    session['tenants']    = [{'id': t.id, 'name': t.name} for t in user.get_tenants()]

    if tenant and tenant.has_member(user):
        session['tenant_id']    = tenant.id
        session['tenant_name']  = tenant.name
        session['tenant_roles'] = tenant.get_roles_for_member(user)
    else:
        session['tenant_id']    = None
        session['tenant_name']  = None
        session['tenant_roles'] = []


def custom_login(user, tenant=None):
    """Full browser login — increments login_count, sets session.
    Never called for API token requests."""
    from app.models import User, Tenant, db
    if not isinstance(user, User):
        return

    user.login_count = (user.login_count or 0) + 1
    db.session.commit()
    login_user(user)

    if tenant is not None:
        tenant = Tenant.query.get(getattr(tenant, 'id', tenant))

    if tenant is None:
        tenants = user.get_tenants()
        tenant  = tenants[0] if tenants else None

    set_session_data(user, tenant)


# ── Token validation ──────────────────────────────────────────────────────────

def _validate_token(token_value):
    """Validate an API token and return the user, or None.
    Pure validation — no session writes, no login_count, no side effects."""
    from app.models import User
    user = User.verify_auth_token(token_value)
    if not user:
        return None
    if not user.is_active:
        return None
    if not user.email_confirmed_at:
        return None
    return user


# ── Request context setup ─────────────────────────────────────────────────────

def _setup_token_context(user):
    """Mark this request as token-authenticated.
    Sets flask.g fields that the Authorizer reads.
    No session is created. No DB writes."""
    g.auth_mode = AUTH_TOKEN
    g.auth_user = user


def _setup_session_context():
    """Mark this request as session-authenticated.
    The Authorizer will read tenant/roles from the Flask session."""
    g.auth_mode = AUTH_SESSION
    g.auth_user = current_user._get_current_object()


# ── Decorators ────────────────────────────────────────────────────────────────

def login_required(view_function):
    """Authenticate the request via either session (browser) or token (API).

    Browser path:
        - Uses Flask-Login session
        - Authorizer reads tenant_id and roles from session
        - Full redirect logic for unconfirmed email, password change, etc.

    API token path:
        - Validates token from the `token` HTTP header
        - Stateless: no session created, no login_count increment
        - Authorizer resolves tenant membership from the URL on first use
        - Email confirmation and password change checks are skipped
          (the token itself proves the user was authenticated)
    """

    @wraps(view_function)
    def decorator(*args, **kwargs):

        # ── API token path ────────────────────────────────────────────
        if token_value := request.headers.get("token"):
            user = _validate_token(token_value)
            if not user:
                return jsonify({"message": "Invalid or expired token"}), 401
            login_user(user, remember=False)
            _setup_token_context(user)
            return view_function(*args, **kwargs)

        # ── Browser session path ──────────────────────────────────────
        if not current_user.is_authenticated:
            return redirect(url_for("auth.get_login", next=request.full_path))

        if not current_user.is_active:
            logout_user()
            session.clear()
            flash("User account is disabled", "warning")
            return redirect(url_for("auth.get_login"))

        if not current_user.email_confirmed_at and request.endpoint not in [
            "api.send_user_confirmation",
            "api.verify_user_confirmation",
            "api.add_tenant",
            "api.set_session"
        ]:
            flash("Please confirm your email to continue")
            return redirect(url_for("auth.get_register"))

        if current_user.is_password_change_required() and request.endpoint not in [
            "auth.set_password",
            "api.change_password",
        ]:
            return redirect(url_for("auth.set_password"))

        _setup_session_context()
        return view_function(*args, **kwargs)

    return decorator


def is_logged_in(f):
    """Redirect already-authenticated users (e.g., away from login page)."""
    @wraps(f)
    def decorated_function(*args, **kws):
        next_page = request.args.get("next")
        if current_user.is_authenticated:
            return redirect(next_page or url_for("main.home"))
        return f(*args, **kws)
    return decorated_function


def internal_api_required(f):
    """Verify requests come from internal workers via shared secret."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("X-Internal-Secret", "")
        expected = current_app.config.get("INTERNAL_API_SECRET", "")
        if not token or token != expected:
            abort(403, "Unauthorized internal request")
        return f(*args, **kwargs)
    return decorated