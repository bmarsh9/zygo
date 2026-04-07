from flask import Flask, request, render_template, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_mail import Mail
from config import config
from flask_migrate import Migrate
from flask_login import LoginManager
from authlib.integrations.flask_client import OAuth
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from sqlalchemy import exc
import logging


db = SQLAlchemy()
migrate = Migrate()
mail = Mail()
login = LoginManager()
login.login_view = "auth.get_login"
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],
    storage_uri="memory://",
)

def create_app(config_name="default"):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

    configure_models(app)
    registering_blueprints(app)
    configure_extensions(app)
    configure_auth_providers(app)
    configure_errors(app)
    configure_logging(app)
    set_config_options(app)

    """
    @app.before_request
    def before_request():
        pass
    """

    return app


def configure_auth_providers(app):
    oauth = OAuth(app)
    app.providers = {}
    app.providers["google"] = oauth.register(
        name="google",
        client_id=app.config.get("GOOGLE_CLIENT_ID"),
        client_secret=app.config.get("GOOGLE_CLIENT_SECRET"),
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )
    app.providers["microsoft"] = oauth.register(
        name="microsoft",
        client_id=app.config.get("MICROSOFT_CLIENT_ID"),
        client_secret=app.config.get("MICROSOFT_CLIENT_SECRET"),
        authorize_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
        authorize_params=None,
        access_token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
        access_token_params=None,
        client_kwargs={"scope": "openid email profile"},
        jwks_uri="https://login.microsoftonline.com/common/discovery/v2.0/keys",
    )
    app.is_google_auth_configured = False
    app.is_microsoft_auth_configured = False

    if (
        app.config["ENABLE_GOOGLE_AUTH"]
        and app.config["GOOGLE_CLIENT_ID"]
        and app.config["GOOGLE_CLIENT_SECRET"]
    ):
        app.is_google_auth_configured = True

    if (
        app.config["ENABLE_MICROSOFT_AUTH"]
        and app.config["MICROSOFT_CLIENT_ID"]
        and app.config["MICROSOFT_CLIENT_SECRET"]
    ):
        app.is_microsoft_auth_configured = True


def configure_models(app):
    from app import models
    from app.utils.authorizer import init_authorizer

    app.models = {
        name: getattr(models, name)
        for name in dir(models)
        if isinstance(getattr(models, name), type)
    }
    app.db = db
    init_authorizer({
        "User":            models.User,
        "Tenant":          models.Tenant,
        "TenantMember":    models.TenantMember,
        "Flow":            models.Flow,
        "Run":             models.Run,
        "Credential":      models.Credential,
        "Dashboard":       models.Dashboard,
        "Ticket":          models.Ticket,
        "DataTable":       models.DataTable,
        "DataRecord":      models.DataRecord,
    })
    return


def configure_extensions(app):
    db.init_app(app)
    mail.init_app(app)
    migrate.init_app(app, db)
    login.init_app(app)
    limiter.init_app(app)

    # Redis + RQ
    import redis
    from rq import Queue
    app.redis_conn = redis.from_url(app.config.get("REDIS_URL"))
    app.task_queue = Queue(connection=app.redis_conn)

    @app.context_processor
    def inject_user():
        from flask_login import current_user
        return dict(current_user=current_user)

    @app.before_request
    def refresh_stale_session():
        from flask import session
        from flask_login import current_user
        if current_user.is_authenticated and current_user.session_stale:
            from app.utils.decorators import set_session_data
            from app.models import Tenant
            tenant_id = session.get("tenant_id")
            tenant = Tenant.query.get(tenant_id) if tenant_id else None
            set_session_data(current_user, tenant)
            current_user.session_stale = False
            db.session.commit()

    return


def registering_blueprints(app):
    from app.main import main as main_blueprint
    app.register_blueprint(main_blueprint)

    from app.api_v1 import api as api_v1_blueprint
    app.register_blueprint(api_v1_blueprint, url_prefix="/api")

    from app.api_v1 import internal_api as api_v1_internal_blueprint
    app.register_blueprint(api_v1_internal_blueprint, url_prefix="/internal/api")

    from app.auth import auth as auth_blueprint
    app.register_blueprint(auth_blueprint)
    return


def configure_errors(app):
    def handle_error(e, title):
        """Generic error handler for API and HTML responses."""
        if request.path.startswith("/api/"):
            response = (
                e.description
                if isinstance(e.description, dict)
                else {"ok": False, "message": e.description, "code": e.code}
            )
            return jsonify(response), e.code

        return (
            render_template(
                "layouts/errors/default.html", title=title, description=e.description
            ),
            e.code,
        )

    @app.errorhandler(405)
    def invalid_method(e):
        return handle_error(e, "Invalid method")

    @app.errorhandler(422)
    def client_error(e):
        return handle_error(e, "Client: bad request")

    @app.errorhandler(404)
    def not_found(e):
        return handle_error(e, "Not found")

    @app.errorhandler(400)
    def bad_request(e):
        return handle_error(e, "Bad request")

    @app.errorhandler(401)
    def not_authenticated(e):
        return handle_error(e, "Unauthenticated")

    @app.errorhandler(403)
    def not_authorized(e):
        return handle_error(e, "Unauthorized")

    @app.errorhandler(500)
    def internal_error(e):
        return handle_error(e, "Internal error")

    @app.errorhandler(exc.SQLAlchemyError)
    def handle_db_exceptions(e):
        app.logger.warning(f"Rolling back database session in app. Error: {e}")
        db.session.rollback()

        try:
            error = str(e.orig)
        except:
            error = "Something went wrong"

        if request.path.startswith("/api/"):
            return jsonify({"ok": False, "message": error, "code": 500}), 500
        return (
            render_template("layouts/errors/default.html", title="Internal error"),
            500,
        )


def configure_logging(app):
    """Configures logging for Flask with fallback to standard logging if GCP logging fails."""

    # Clear existing handlers to avoid duplicate logs
    app.logger.handlers.clear()

    if app.config.get("ENABLE_GCP_LOGGING", False):
        try:
            from google.cloud import logging as gcloud_logging

            gcloud_client = gcloud_logging.Client()
            gcloud_client.setup_logging()

            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '{"message": "%(message)s", "severity": "%(levelname)s"}'
            )
            handler.setFormatter(formatter)

            app.logger.addHandler(handler)
            app.logger.setLevel(app.config["LOG_LEVEL"])

            app.logger.info("Enabled GCP logging")
            return
        except Exception as e:
            app.logger.error(f"Failed to configure GCP Logging, falling back: {e}")

    # Fallback to Standard Logging
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    app.logger.addHandler(handler)
    app.logger.setLevel(app.config["LOG_LEVEL"])

    app.logger.info("Enabled standard Flask logging")


def set_config_options(app):
    app.is_email_configured = False
    app.is_self_registration_enabled = False

    if app.config.get("EMAIL_PROVIDER") == "sender":
        if app.config.get("SENDER_API_TOKEN"):
            app.is_email_configured = True
    else:
        if app.config.get("MAIL_USERNAME") and app.config.get("MAIL_PASSWORD"):
            app.is_email_configured = True

    if app.is_email_configured and app.config.get("ENABLE_SELF_REGISTRATION"):
        app.is_self_registration_enabled = True

    app.config["settings"] = {
        "email_provider": app.config.get("EMAIL_PROVIDER"),
        "is_self_registration_enabled": app.is_self_registration_enabled,
        "is_email_configured": app.is_email_configured
    }
