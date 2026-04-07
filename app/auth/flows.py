from flask import abort, flash, redirect, url_for, session, current_app
from app.utils.decorators import custom_login
from app.models import db, User
from app.utils import misc


class UserFlow:
    """Handles different user authentication flows: login, register, accept."""

    VALID_FLOW_TYPES = ["login", "register", "accept"]
    PROVIDERS = ["local", "google", "microsoft"]

    def __init__(self, user_info, flow_type, provider, next_page=None):
        """
        user_info: user info that is provided by the provider
        flow_type: describes the user flow type - see VALID_FLOW_TYPES
        provider: valid provider - see PROVIDERS
        next_page: next page to redirect, expects url_for(<path>)
        """
        self.user_info = user_info
        self.user_dict = {}
        self.flow_type = flow_type
        self.provider = provider
        self.next_page = next_page

        self.validate_flow()
        self.validate_provider()

        # Normalize different user_info into common schema
        if provider == "google":
            self.parse_google_info()
        elif provider == "microsoft":
            self.parse_microsoft_info()
        else:
            self.parse_local_info()

    def parse_google_info(self):
        self.user_dict = {
            "email": self.user_info.get("email"),
            "first": self.user_info.get("given_name"),
            "last": self.user_info.get("family_name"),
        }
        if not self.user_dict["email"]:
            abort(422, "Missing required field: email")

    def parse_microsoft_info(self):
        self.user_dict = {
            "email": self.user_info.get("email"),
            "first": self.user_info.get("name"),
            "last": None,
        }
        if not self.user_dict["email"]:
            abort(422, "Missing required field: email")

    def parse_local_info(self):
        self.user_dict = {
            "email": self.user_info.get("email"),
            "display_name": self.user_info.get("display_name"),
            "password": self.user_info.get("password"),
            "password2": self.user_info.get("password2"),
        }
        if not self.user_dict["email"]:
            abort(422, "Missing required field: email")

        if not self.user_dict["password"]:
            abort(422, "Missing required field: password")

        if self.flow_type == "register" and not self.user_dict["password2"]:
            abort(422, "Missing required field: password2")

    def validate_flow(self):
        if self.flow_type not in self.VALID_FLOW_TYPES:
            abort(422, "Invalid flow type")

    def validate_provider(self):
        if self.provider not in self.PROVIDERS:
            abort(422, "Invalid provider")

    def handle_flow(self, attributes={}):
        """Routes authentication based on flow type."""
        if self.flow_type == "login":
            return self._handle_login()
        elif self.flow_type == "register":
            return self._handle_register()
        elif self.flow_type == "accept":
            return self._handle_accept(**attributes)
        abort(403, "Invalid authentication flow")

    def _handle_login(self, user=None):
        """Handles login flow."""
        if user is None:
            user = User.find_by_email(self.user_dict["email"])

        if not user:
            # If user does not exist but self-service registration is enabled, redirect
            if current_app.is_self_registration_enabled:
                flash("Unable to find account. Please create one.", "warning")
                return redirect(url_for("auth.get_register", provider=self.provider, email=self.user_dict.get("email")))
            abort(403, "Unable to find account. Registration is disabled.")

        if self.provider == "local" and not user.check_password(
            self.user_dict["password"]
        ):
            abort(403, "Invalid password or email")

        custom_login(user)

        # Have user create tenant if they don't have any
        if get_started := self.should_we_create_tenant(user):
            return get_started

        return redirect(self.next_page or url_for("main.home"))

    def _handle_register(self):
        """Handles self-service registration flow (federated providers).
        Local registration is now handled by the JSON endpoint in views.py.
        This method handles Google/Microsoft OAuth registration callbacks.
        """
        if current_app.is_self_registration_enabled is not True:
            abort(403, "Self-service registration is disabled")

        # User already exists, follow login path
        if user := User.find_by_email(self.user_dict["email"]):
            custom_login(user)
            # Redirect to the wizard — it will detect user state and show the right step
            return redirect(url_for("auth.get_register"))

        # Build display name from provider info
        display_name = None
        if self.provider == "google":
            parts = [self.user_dict.get("first"), self.user_dict.get("last")]
            display_name = " ".join(p for p in parts if p)
        elif self.provider == "microsoft":
            display_name = self.user_dict.get("first")

        user_object = {
            "email": self.user_dict["email"],
            "display_name": display_name,
            "password": self.user_dict.get("password", None),
            "confirmed": True,  # Federated users are auto-confirmed
            "return_user_object": True,
        }

        user = User.add(**user_object)
        custom_login(user)

        # Redirect to the registration wizard — it will pick up at step 2 (create tenant)
        return redirect(url_for("auth.get_register"))

    def _handle_accept(self, token):
        """Handles user accepting tenant invite flow."""
        if not (result := User.verify_invite_token(token)):
            abort(403, "Invalid or expired invite token")

        if not (user := User.find_by_email(result.get("email"))):
            abort(403, "Invalid token: email not found")

        user.display_name = self.user_dict["display_name"] or user.display_name

        # For local provider, we set up the users password
        if self.provider == "local":
            if not misc.perform_pwd_checks(
                self.user_dict.get("password"),
                password_two=self.user_dict.get("password2"),
            ):
                flash("Invalid password.", "warning")
                return redirect(url_for("auth.get_accept", token=token))
            user.set_password(self.user_dict.get("password"), set_pwd_change=True)
            user.set_confirmation()

        db.session.commit()
        custom_login(user)
        return redirect(self.next_page or url_for("main.home"))

    def should_we_create_tenant(self, user):
        """Redirect users without tenants to the registration wizard."""
        if not user.get_tenants():
            return redirect(url_for("auth.get_register"))
        return False