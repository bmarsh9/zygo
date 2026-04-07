from flask import render_template, current_app
from . import main
from flask_login import current_user
from app.utils.decorators import login_required
from app.utils.authorizer import Authorizer

# ── System endpoints ─────────────────────────────────────────────────────────────────────
@main.route("/system/users", methods=["GET"])
@login_required
def system_users():
    Authorizer(current_user).assert_super()
    return render_template("system/users.html")


@main.route("/system/settings", methods=["GET"])
@login_required
def system_settings():
    Authorizer(current_user).assert_super()
    return render_template("system/settings.html")


@main.route("/system/logs", methods=["GET"])
@login_required
def system_logs():
    Authorizer(current_user).assert_super()
    return render_template("system/logs.html", system=True)


# ── User endpoints ─────────────────────────────────────────────────────────────────────
@main.route("/users/<string:user_id>", methods=["GET"])
@login_required
def view_user(user_id):
    return render_template("system/user_profile.html", user_id=user_id)

# ── Tenant endpoints ─────────────────────────────────────────────────────────────────────
@main.route("/tenants", methods=["GET"])
@login_required
def tenants():
    return render_template("system/tenants.html")

@main.route("/users", methods=["GET"])
@login_required
def tenant_users():
    return render_template("system/tenant_users.html")

@main.route("/logs", methods=["GET"])
@login_required
def tenant_logs():
    return render_template("system/logs.html")


@main.route("/docs", methods=["GET"])
@login_required
def get_api_docs():
    """Serve Swagger UI pointing at the spec."""
    return render_template("system/api_docs.html")

