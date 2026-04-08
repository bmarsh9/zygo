from flask import (
    jsonify,
    request,
    current_app,
    abort,
    render_template,
    session,
)
from . import api
from app import models, db
from flask_login import current_user
from app.utils.decorators import login_required, set_session_data
from app.email import send_template_email
from app.utils.authorizer import Authorizer, _can_create_tenant
from app.utils import misc
import os


# ── Health ────────────────────────────────────────────────────────────────────

@api.route("/health", methods=["GET"])
def get_health():
    return jsonify({"message": "ok"})


# ── Misc helpers ──────────────────────────────────────────────────────────────

@api.route("/users/exist", methods=["POST"])
def does_user_exist():
    data = request.get_json()
    if not data.get("email"):
        abort(404)
    user = models.User.find_by_email(data.get("email"))
    if not user:
        abort(404)
    return jsonify({"message": True})


@api.route("/email-check", methods=["GET"])
@login_required
def check_email():
    Authorizer(current_user).assert_super()
    response = send_template_email(
        "Email Check",
        recipients=[current_user.email],
        content="Email health check is successful",
        button_link=current_app.config["HOST_NAME"],
        button_label="Login",
    )
    return jsonify({"message": "Email health attempt", "success": response})

@api.route("/users/<string:uid>/welcome-dismissed", methods=["PUT"])
@login_required
def dismiss_welcome(uid):
    if str(current_user.id) != uid:
        abort(403)
    current_user.has_seen_welcome = True
    db.session.commit()
    return jsonify({"message": "ok"})

# ── Session ───────────────────────────────────────────────────────────────────

@api.route("/session", methods=["GET"])
@login_required
def get_session():
    return jsonify({
        "tenant_id":    session.get("tenant_id"),
        "tenant_name":  session.get("tenant_name"),
        "tenant_roles": session.get("tenant_roles", []),
        "user_id":      session.get("user_id"),
        "user_email":   session.get("user_email"),
        "user_super":   session.get("user_super"),
    })


@api.route("/session/<string:tenant_id>", methods=["PUT"])
@login_required
def set_session(tenant_id):
    tenant = Authorizer(current_user).switch_tenant(tenant_id)
    set_session_data(current_user, tenant)
    session.modified = True
    return jsonify({"message": "ok"})


@api.route("/session/refresh", methods=["POST"])
@login_required
def refresh_session():
    auth = Authorizer(current_user)
    tenant = auth.tenant()
    set_session_data(current_user, tenant)
    db.session.commit()
    return jsonify(get_session().get_json())


@api.route("/session", methods=["DELETE"])
@login_required
def delete_session():
    session.clear()
    return jsonify({"message": "ok"})


# ── Users ─────────────────────────────────────────────────────────────────────

@api.route("/users", methods=["GET"])
@login_required
def get_users():
    Authorizer(current_user).assert_super()
    return jsonify([user.as_dict() for user in models.User.query.all()])


@api.route("/users", methods=["POST"])
@login_required
def create_user():
    Authorizer(current_user).assert_super()
    data = request.get_json()
    response = models.User.add(
        email=data.get("email"), super=True, send_notification=True
    )
    return jsonify(response)


@api.route("/users/<string:user_id>", methods=["GET"])
@login_required
def get_user(user_id):
    auth = Authorizer(current_user)
    user = auth.own_user(user_id)
    return jsonify(user.as_dict())


@api.route("/users/<string:user_id>", methods=["PUT"])
@login_required
def update_user(user_id):
    auth = Authorizer(current_user)
    user = auth.own_user(user_id)
    data = request.get_json()

    user.email        = data.get("email", user.email)
    user.display_name = data.get("display_name", user.display_name)
    user.license      = data.get("license", user.license)
    user.trial_days   = int(data.get("trial_days", user.trial_days))

    if auth.is_super:
        if "is_active" in data:
            user.is_active = data.get("is_active")
        if "super" in data:
            user.super = data.get("super")
        if "can_user_create_tenant" in data:
            user.can_user_create_tenant = data.get("can_user_create_tenant")
        if "tenant_limit" in data:
            user.tenant_limit = int(data.get("tenant_limit"))

    if data.get("email_confirmed") is True and not user.email_confirmed_at:
        user.set_confirmation()

    if data.get("email_confirmed") is False:
        user.email_confirmed_at = None

    db.session.commit()
    return jsonify({"message": user.as_dict()})

@api.route("/users/<string:user_id>/password", methods=["PUT"])
@login_required
def change_password(user_id):
    auth = Authorizer(current_user)
    user = auth.own_user(user_id)
    data = request.get_json()
    password  = data.get("password")
    password2 = data.get("password2")
    if not misc.perform_pwd_checks(password, password_two=password2):
        abort(422, "Invalid password")
    user.set_password(password, set_pwd_change=True)
    db.session.commit()
    return jsonify({"message": "Successfully updated the password"})

@api.route("/users/<string:user_id>", methods=["DELETE"])
@login_required
def delete_user(user_id):
    auth = Authorizer(current_user)
    user = auth.own_user(user_id)
    user.is_active = False
    db.session.commit()
    return jsonify({"message": "ok"})


@api.route("/users/<string:user_id>/send-confirmation", methods=["POST"])
@login_required
def send_user_confirmation(user_id):
    auth = Authorizer(current_user)

    # Allowed if: own user, superadmin, or admin in the same session tenant as target user
    target = models.User.query.get(user_id)
    if not target:
        abort(404)

    if not auth.is_super and auth.user.id != user_id:
        shared = models.TenantMember.query.filter_by(
            user_id=user_id,
            tenant_id=auth.tenant_id,
        ).first()
        if not shared or not auth.has_role("admin"):
            abort(403, description="You do not have permission to send confirmation emails for this user")

    target.send_email_confirmation()
    return jsonify({"message": "ok"})


@api.route("/users/<string:user_id>/verify-confirmation-code", methods=["POST"])
@login_required
def verify_user_confirmation(user_id):
    # Only the user themselves can verify their own confirmation code
    auth = Authorizer(current_user)
    user = auth.own_user(user_id)
    data = request.get_json()
    if data.get("code", "").strip() != user.email_confirm_code:
        abort(403, "Invalid confirmation code")
    user.set_confirmation()
    db.session.commit()
    return jsonify({"message": "ok"})


@api.route("/token", methods=["GET"])
@login_required
def generate_api_token():
    expiration = int(request.args.get("expiration", 600))
    token = current_user.generate_auth_token(expiration=expiration)
    return jsonify({"token": token, "expires_in": expiration})


# ── Tenants ───────────────────────────────────────────────────────────────────

@api.route("/tenants/<string:tenant_id>", methods=["GET"])
@login_required
def get_tenant(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="viewer")
    return jsonify(auth.tenant().as_dict())


@api.route("/tenants/<string:tenant_id>", methods=["DELETE"])
@login_required
def delete_tenant(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="admin")
    auth.tenant().delete()
    return jsonify({"message": "ok"})


@api.route("/tenants/<string:tenant_id>", methods=["PUT"])
@login_required
def update_tenant(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="admin")
    tenant = auth.tenant()
    data = request.get_json()

    if data.get("contact_email"):
        tenant.contact_email = data.get("contact_email")

    if data.get("magic_link_login") in [True, False]:
        tenant.magic_link_login = data.get("magic_link_login")

    if "approved_domains" in data:
        approved_domains = data.get("approved_domains")
        if isinstance(approved_domains, list):
            tenant.approved_domains = ", ".join(approved_domains)
        elif isinstance(approved_domains, str):
            tenant.approved_domains = approved_domains

    # Platform-only fields — require superadmin
    if any(key in data for key in ["license", "flow_cap"]):
        auth.assert_super()
        tenant.license     = data.get("license", tenant.license)
        tenant.flow_cap = int(data.get("flow_cap", tenant.flow_cap))

    db.session.commit()
    return jsonify(tenant.as_dict())


@api.route("/tenants", methods=["GET"])
@login_required
def get_tenants():
    return jsonify([t.as_dict() for t in current_user.get_tenants()])


@api.route("/tenants", methods=["POST"])
@login_required
def add_tenant():
    # Inline check — uses a user-level flag, not a role
    if not _can_create_tenant(current_user):
        abort(403, description="You are not allowed to create tenants")
    data = request.get_json()
    try:
        tenant = models.Tenant.create(
            current_user,
            data.get("name"),
            data.get("contact_email"),
            approved_domains=data.get("approved_domains"),
        )
    except Exception as e:
        return jsonify({"message": str(e)}), 400
    return jsonify(tenant.as_dict())


@api.route("/users/<string:user_id>/tenants", methods=["GET"])
@login_required
def get_tenants_for_user(user_id):
    auth = Authorizer(current_user)
    user = auth.own_user(user_id)
    return jsonify([{"id": t.id, "name": t.name} for t in user.get_tenants()])


@api.route("/tenants/<string:tenant_id>/users", methods=["GET"])
@login_required
def get_users_for_tenant(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="viewer")
    return jsonify(auth.tenant().get_members())


@api.route("/users/<string:user_id>/tenants/<string:tenant_id>/roles", methods=["GET"])
@login_required
def get_roles_for_user_in_tenant(user_id, tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="viewer")
    user = models.User.query.get(user_id)
    if not user:
        abort(404)
    return jsonify(user.all_roles_by_tenant(auth.tenant()))


@api.route("/tenants/<string:tenant_id>/users/<string:user_id>", methods=["PUT"])
@login_required
def update_user_in_tenant(tenant_id, user_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="admin")
    user = auth.tenant_user(user_id, role="admin")
    data = request.get_json()

    user.email        = data.get("email", user.email)
    user.display_name = data.get("display_name", user.display_name)
    user.license      = data.get("license", user.license)
    user.trial_days   = int(data.get("trial_days", user.trial_days))

    if roles := data.get("roles"):
        auth.tenant().patch_roles_for_member(user, roles)

    db.session.commit()
    return jsonify({"message": "ok"})


@api.route("/tenants/<string:tenant_id>/users/<string:user_id>", methods=["DELETE"])
@login_required
def remove_user_from_tenant(tenant_id, user_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="admin")
    user = auth.tenant_user(user_id, role="admin")
    auth.tenant().remove_member(user)
    return jsonify({"message": "ok"})


@api.route("/tenants/<string:tenant_id>/users", methods=["POST"])
@login_required
def add_user_to_tenant(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="admin")
    data = request.get_json()
    response = auth.tenant().add_member(
        user_or_email=data.get("email"),
        attributes={"roles": data.get("roles", [])},
        send_notification=True,
    )
    return jsonify(response)


@api.route("/tenants/<string:tenant_id>/chat", methods=["POST"])
@login_required
def post_ai_conversation(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    return jsonify({"source": "server", "message": "We are still in beta! Coming soon!"})


@api.route("/feedback", methods=["POST"])
@login_required
def submit_feedback():
    data = request.get_json() or {}
    message = data.get("message", "").strip()
    if not message:
        return jsonify({"message": "Message is required"}), 400

    email = data.get("email", current_user.email)
    page = data.get("page", "")

    models.Logs.add(
        message=f"Feedback from {email}: {message}",
        namespace="feedback",
        user_id=current_user.id,
    )

    return jsonify({"message": "ok"})
# ── Logs ──────────────────────────────────────────────────────────────────────

@api.route("/logs")
@login_required
def get_logs():
    Authorizer(current_user).assert_super()
    return jsonify(models.Logs.get(as_dict=True, limit=500))


@api.route("/tenants/<string:tenant_id>/logs")
@login_required
def get_logs_for_tenant(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="viewer")
    return jsonify(models.Logs.get(tenant_id=auth.tenant_id, as_dict=True, limit=500))

@api.route("/docs/openapi.yaml", methods=["GET"])
@login_required
def get_openapi_spec():
    """Serve the raw OpenAPI spec."""
    spec_path = os.path.join(current_app.root_path, "openapi.yaml")
    with open(spec_path) as f:
        return f.read(), 200, {"Content-Type": "text/yaml"}