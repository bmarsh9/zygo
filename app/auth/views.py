from flask import (
    request,
    flash,
    redirect,
    url_for,
    session,
    jsonify,
    abort,
    current_app,
)
from flask_login import current_user, logout_user
from app.utils.decorators import custom_login, login_required, is_logged_in
from . import auth
from app.models import *
from app.utils import misc
from app.auth.flows import UserFlow
from app import limiter


@auth.route("/login", methods=["GET"])
@is_logged_in
def get_login():
    return render_template("auth/login.html")


@auth.route("/login", methods=["POST"])
@is_logged_in
@limiter.limit("10/minute")
def post_login():
    """
    JSON endpoint for login. Returns user state or error message.
    """
    next_page = request.args.get("next")
    data = request.get_json()
    if not data:
        return jsonify({"message": "Invalid request"}), 400

    email = data.get("email", "").strip()
    password = data.get("password", "")

    if not email or "@" not in email:
        return jsonify({"message": "Valid email is required"}), 400
    if not password:
        return jsonify({"message": "Password is required"}), 400

    user = User.find_by_email(email)
    if not user:
        if current_app.is_self_registration_enabled:
            return jsonify({
                "message": "Invalid email or password",
                "redirect": url_for("auth.get_register", email=email),
            }), 403
        return jsonify({"message": "Invalid email or password"}), 403

    if not user.check_password(password):
        return jsonify({"message": "Invalid email or password"}), 403

    if not user.is_active:
        return jsonify({"message": "Account is disabled"}), 403

    custom_login(user)

    # Determine where to send the user
    has_tenants = len(user.get_tenants()) > 0
    redirect_to = next_page or url_for("main.home")

    if not has_tenants:
        redirect_to = url_for("auth.get_register")
    elif not user.email_confirmed_at:
        redirect_to = url_for("auth.get_register")
    elif user.is_password_change_required():
        redirect_to = url_for("auth.set_password")

    return jsonify({
        "redirect": redirect_to,
    })


@auth.route("/logout", methods=["POST"])
def logout():
    logout_user()
    session.clear()
    return redirect(url_for("auth.get_login"))


@auth.route("/login/tenants/<string:tid>", methods=["GET", "POST"])
@is_logged_in
def login_with_magic_link(tid):
    next_page = request.args.get("next")
    if current_user.is_authenticated:
        return redirect(next_page or url_for("main.home"))

    if not current_app.is_email_configured:
        flash("Email is not configured", "warning")
        abort(404)
    if not (tenant := Tenant.query.get(tid)):
        abort(404)
    if not tenant.magic_link_login:
        flash("Feature is not enabled", "warning")
        abort(404)
    if request.method == "POST":
        email = request.form["email"]
        if not (user := User.find_by_email(email)):
            flash("Invalid email", "warning")
            tenant.add_log(message=f"invalid email for {email}", level="warning")
            return redirect(url_for("auth.login_with_magic_link", tid=tid))
        if not user.is_active:
            flash("User is inactive", "warning")
            tenant.add_log(
                message=f"inactive user tried to login:{email}",
                level="warning",
            )
            return redirect(next_page or url_for("auth.login_with_magic_link", tid=tid))

        token = user.generate_magic_link(tid)
        send_template_email(
            "Login Request",
            recipients=[email],
            content="You have requested a login via email. If you did not request a magic link, please ignore. Otherwise, please click the button below to login.",
            button_link=f"{current_app.config['HOST_NAME']}magic-login/{token}",
            button_label="Login",
        )
        tenant.add_log(message=f"magic link login request to {email}")
        flash("Please check your email for the login information")
    return render_template("auth/magic-login.html", tid=tid)


@auth.route("/magic-login/<string:token>", methods=["GET"])
@is_logged_in
def validate_magic_link(token):
    next_page = request.args.get("next")
    if not (vtoken := User.verify_magic_token(token)):
        flash("Token is invalid", "warning")
        return redirect(url_for("auth.get_login"))
    if not (user := User.query.get(vtoken.get("user_id"))):
        flash("Invalid user id", "warning")
        return redirect(url_for("auth.get_login"))
    if not (tenant := Tenant.query.get(vtoken.get("tenant_id"))):
        flash("Invalid tenant id", "warning")
        return redirect(url_for("auth.get_login"))
    if user.id == tenant.owner_id or user.has_tenant(tenant):
        flash("Welcome")
        Logs.add(message=f"{user.email} logged in via magic link", user_id=user.id)
        custom_login(user)
        return redirect(next_page or url_for("main.home"))
    flash("User can not access tenant", "warning")
    return redirect(url_for("auth.get_login"))


@auth.route("/accept", methods=["GET"])
@is_logged_in
def get_accept():
    """
    GET endpoint for a user accepting invitations
    """
    if not (result := User.verify_invite_token(request.args.get("token"))):
        abort(403, "Invalid or expired invite token")

    if not (user := User.find_by_email(result.get("email"))):
        abort(403, "Invalid token: email not found")

    # If user has already logged in, we show them the login page, otherwise
    # we will show them the accept page (register)
    result["login_count"] = user.login_count
    if user.login_count > 0:
        return redirect(
            url_for(
                "auth.get_login", email=result.get("email"), tenant=result.get("tenant")
            )
        )

    return render_template(
        "auth/accept.html", data=result, token=request.args.get("token")
    )


@auth.route("/accept", methods=["POST"])
@is_logged_in
def post_accept():
    """
    POST endpoint for a user accepting invitations
    """
    next_page = request.args.get("next")
    attributes = {"token": request.args.get("token")}
    return UserFlow(
        user_info=request.form,
        flow_type="accept",
        provider="local",
        next_page=next_page,
    ).handle_flow(attributes)


@auth.route("/reset-password", methods=["GET", "POST"])
@limiter.limit("5/second")
def reset_password_request():
    next_page = request.args.get("next")
    internal = request.args.get("internal")
    if current_user.is_authenticated and not internal:
        return redirect(next_page or url_for("main.home"))

    if not current_app.is_email_configured:
        flash("Email is not configured. Please contact your admin.", "warning")
        return redirect(url_for("main.home"))

    if request.method == "POST":
        email = request.form.get("email")
        if not (user := User.find_by_email(email)):
            flash("If the account is valid, you will receive a email")
            return redirect(next_page or url_for("auth.reset_password_request"))
        Logs.add(
            message=f"{email} requested a password reset",
            level="warning",
            user_id=user.id,
        )
        token = user.generate_auth_token()
        send_template_email(
            "Password reset",
            recipients=[email],
            content="You have requested a password reset. If you did not request a reset, please ignore. Otherwise, click the button below to continue.",
            button_link=f"{current_app.config['HOST_NAME']}reset-password/{token}",
            button_label="Reset",
        )
        flash("If the account is valid, you will receive a email")
        return redirect(url_for("auth.get_login"))
    return render_template("auth/reset_password_request.html")


@auth.route("/reset-password/<string:token>", methods=["GET", "POST"])
def reset_password(token):
    if current_user.is_authenticated:
        return redirect(url_for("main.home"))
    if not (user := User.verify_auth_token(token)):
        Logs.add(
            message="invalid or missing token for password reset",
            level="warning",
            user_id=current_user.id,
        )
        flash("Missing or invalid token", "warning")
        return redirect(url_for("auth.reset_password_request"))
    if request.method == "POST":
        password = request.form.get("password")
        password2 = request.form.get("password2")
        if not misc.perform_pwd_checks(password, password_two=password2):
            flash("Password did not pass checks", "warning")
            return redirect(url_for("auth.reset_password", token=token))
        user.set_password(password, set_pwd_change=True)
        db.session.commit()
        flash("Password reset! Please login with your new password", "success")
        Logs.add(
            message=f"{user.email} reset their password",
            level="warning",
            user_id=user.id,
        )
        return redirect(url_for("auth.get_login"))
    return render_template("auth/reset_password.html", token=token)


@auth.route("/set-password", methods=["GET"])
@login_required
def set_password():
    """
    When a user must set or change their password
    """
    return render_template("auth/set_password.html")


@auth.route("/register", methods=["GET"])
def get_register():
    if current_app.is_self_registration_enabled is not True:
        abort(403, "Self-service registration is disabled")

    # If user is authenticated and has completed the full flow
    # (has tenants + confirmed email), send them home
    if current_user.is_authenticated:
        has_tenants = len(current_user.get_tenants()) > 0
        is_confirmed = current_user.email_confirmed_at is not None
        if has_tenants and is_confirmed:
            return redirect(url_for("main.home"))

    # Build registration state for the wizard.
    # This is populated when a logged-in user lands here (e.g. after OAuth)
    # or empty for a fresh visit.
    registration_state = _build_registration_state()

    return render_template(
        "auth/register.html",
        registration_state=registration_state,
    )


@auth.route("/register", methods=["POST"])
@limiter.limit("10/minute")
def api_register():
    """
    JSON endpoint for the registration wizard (Step 1: create account).
    Returns JSON with user state so the wizard can advance to the next step.
    """
    if current_app.is_self_registration_enabled is not True:
        return jsonify({"message": "Self-service registration is disabled"}), 403

    # Registration secret check
    reg_secret = current_app.config.get("SELF_REGISTRATION_SECRET")
    if reg_secret:
        data_peek = request.get_json(silent=True) or {}
        if data_peek.get("registration_secret", "").strip() != reg_secret:
            return jsonify({"message": "Invalid registration code"}), 403

    # If already authenticated, return current state instead of allowing
    # re-registration or login-as-another-user through this endpoint
    if current_user.is_authenticated:
        return jsonify({
            "user_id": current_user.id,
            "email": current_user.email,
            "display_name": current_user.display_name,
            "confirmed": current_user.email_confirmed_at is not None,
            "has_tenants": len(current_user.get_tenants()) > 0,
        })

    data = request.get_json()
    if not data:
        return jsonify({"message": "Invalid request"}), 400

    email = data.get("email", "").strip()
    display_name = data.get("display_name", "").strip()
    password = data.get("password", "")
    password2 = data.get("password2", "")

    if not email or "@" not in email:
        return jsonify({"message": "Valid email is required"}), 400
    if not display_name:
        return jsonify({"message": "Display name is required"}), 400
    if not misc.perform_pwd_checks(password, password_two=password2):
        return jsonify({"message": "Invalid password"}), 400

    # Check if user already exists — follow login path
    if user := User.find_by_email(email):
        if not user.check_password(password):
            return jsonify({"message": "Invalid password or email"}), 403
        custom_login(user)
        return jsonify({
            "user_id": user.id,
            "email": user.email,
            "display_name": user.display_name,
            "confirmed": user.email_confirmed_at is not None,
            "has_tenants": len(user.get_tenants()) > 0,
        })

    # Create new user
    try:
        user = User.add(
            email=email,
            display_name=display_name,
            password=password,
            confirmed=False,
            return_user_object=True,
        )
    except Exception as e:
        return jsonify({"message": str(e) or "Unable to create account"}), 400

    if not user:
        return jsonify({"message": "Unable to create account"}), 500

    custom_login(user)

    return jsonify({
        "user_id": user.id,
        "email": user.email,
        "display_name": user.display_name,
        "confirmed": user.email_confirmed_at is not None,
        "has_tenants": False,
    }), 201


def _build_registration_state():
    """
    Build a dict describing where the current user is in the registration flow.
    Used to seed the Alpine wizard when the page loads (e.g. after OAuth redirect).
    Returns empty dict if user is not authenticated.
    """
    if not current_user.is_authenticated:
        return {}

    return {
        "user_id": current_user.id,
        "email": current_user.email,
        "display_name": current_user.display_name,
        "confirmed": current_user.email_confirmed_at is not None,
        "has_tenants": len(current_user.get_tenants()) > 0,
    }