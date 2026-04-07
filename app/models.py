from chardet.cli.chardetect import description_of
from sqlalchemy import func
from sqlalchemy.orm import validates

from app.utils.mixin_models import (
    QueryMixin,
    AuthorizerMixin,
)
from flask_login import UserMixin
from flask import current_app, render_template, abort
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from app import db, login
from uuid import uuid4
from app.utils import misc
import arrow
from app.email import send_template_email
import email_validator
import shortuuid
import enum
import json

def _short_id():
    return shortuuid.ShortUUID().random(length=6).lower()

class Tenant(db.Model, QueryMixin, AuthorizerMixin):
    __tablename__ = "tenants"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    name = db.Column(db.String, nullable=False)
    contact_email = db.Column(db.String())
    license = db.Column(
        db.String(),
        server_default="free"
    )
    flow_cap = db.Column(db.Integer, default=5)
    approved_domains = db.Column(db.String())
    magic_link_login = db.Column(db.Boolean(), default=False)
    members = db.relationship(
        "TenantMember", backref="tenant", lazy="dynamic", cascade="all, delete-orphan"
    )
    owner_id = db.Column(db.String, db.ForeignKey("users.id"), nullable=False)
    date_added = db.Column(db.DateTime, default=datetime.utcnow)
    date_updated = db.Column(db.DateTime, onupdate=datetime.utcnow)

    VALID_LICENSE = ["free", "silver", "gold"]

    @validates("contact_email")
    def _validate_email(self, key, address):
        if address:
            try:
                email_validator.validate_email(address, check_deliverability=False)
            except:
                abort(422, "Invalid email")
        return address

    @validates("name")
    def _validate_name(self, key, name):
        special_characters = "!\"#$%&'()*+,-./:;<=>?@[\]^`{|}~"
        if any(c in special_characters for c in name):
            raise ValueError("Illegal characters in name")
        return name

    def as_dict(self):
        data = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        data["owner_email"] = self.get_owner_email()
        data["approved_domains"] = []
        if self.approved_domains:
            data["approved_domains"] = self.approved_domains.split(",")
        data["date_added"] = _fmt_dt(self.date_added)
        data["date_updated"] = _fmt_dt(self.date_updated)
        return data

    def add_log(self, **kwargs):
        return Logs.add(tenant_id=self.id, **kwargs)

    def get_logs(self, **kwargs):
        return Logs.get(tenant_id=self.id, **kwargs)

    def get_members(self):
        members = (
            TenantMember.query
            .filter_by(tenant_id=self.id)
            .options(
                db.joinedload(TenantMember.user),
                db.joinedload(TenantMember.roles)
            )
            .all()
        )
        result = []
        for member in members:
            user = member.user.as_dict(tenant=self)
            user["roles"] = [role.name for role in member.roles]
            user.pop("tenants", None)
            result.append(user)
        return result

    def send_member_email_invite(self, user):
        response = {"access_link": None, "sent-email": False, "message": None}  # ← add message to default

        if not self.has_member(user):
            response["message"] = "User is not a member of tenant"
            return response

        token = User.generate_invite_token(
            email=user.email, expiration=604800, attributes={"tenant": self.name}
        )
        link = "{}{}?token={}".format(current_app.config["HOST_NAME"], "accept", token)
        response["access_link"] = link

        if not current_app.is_email_configured:
            response["message"] = "Email is not configured"
            return response

        send_template_email(
            "Tenant Invite",
            recipients=[user.email],
            content=f"You have been added to a new tenant: {self.name.capitalize()}",
            button_link=link
        )

        response["sent-email"] = True
        response["message"] = "Invite sent successfully"
        return response

    def has_member(self, user_or_email, get_user_object=False):
        if (
                isinstance(user_or_email, TenantMember)
                and user_or_email.tenant_id == self.id
        ):
            if get_user_object:
                return user_or_email.user
            return user_or_email

        if not (user := User.email_to_object(user_or_email)):
            return None

        member = self.members.filter(TenantMember.user_id == user.id).first()
        if not member:
            return None
        if get_user_object:
            return user
        return member

    def get_roles_for_member(self, user_or_email):
        member = self.has_member(user_or_email)
        if not member:
            return []
        return [role.name for role in member.roles]

    def has_member_with_role(self, user_or_email, role_name):
        if not role_name:
            return False
        member = self.has_member(user_or_email)
        if not member:
            return False
        return role_name.lower() in [role.name.lower() for role in member.roles]

    def add_member(
        self,
        user_or_email,
        attributes=None,
        return_user_object=False,
        send_notification=False,
    ):
        """
        Add user to the tenant. If user does not exist, they will be created and then added to tenant

        user_or_email: user object or email address
        attributes: see User class
        send_notification: send email notification
        return_user_object: return user object (default is dictionary)
        """
        attributes = attributes or {}
        roles = self.get_default_roles(attributes.pop("roles", None))

        # User already exists
        if isinstance(user_or_email, User):
            user = user_or_email
            email = user.email

        # User does not exist
        else:
            email = user_or_email
            user = User.find_by_email(email)

        can_we_invite, error = self.can_we_invite_user(email, user=user)
        if not can_we_invite:
            abort(500, error)

        # If the user does not exist, create them
        if not user:
            user = User.add(email, **attributes, return_user_object=True)

        new_member = TenantMember(user_id=user.id, tenant_id=self.id)
        db.session.add(new_member)
        db.session.commit()

        # Set roles for the member
        self.patch_roles_for_member(user, role_names=roles)

        response = {
            "id": user.id,
            "success": True,
            "message": f"Added {user.email} to {self.name}",
            "sent-email": False,
            "confirm_code": user.email_confirm_code,
        }
        if send_notification:
            email_invite = self.send_member_email_invite(user)
            response["sent-email"] = email_invite["sent-email"]
            response["access_link"] = email_invite["access_link"]

        if return_user_object:
            return user

        return response

    def patch_roles_for_member(self, user, role_names):
        member = self.has_member(user)
        if not member:
            raise ValueError(f"User {user.email} is not a member of {self.name}")

        member.roles = Role.query.filter(
            func.lower(Role.name).in_([r.lower() for r in role_names])
        ).all()
        user.session_stale = True
        db.session.commit()
        return member

    def remove_member(self, user):
        """
        Removes a user from the tenant.
        """
        member = self.has_member(user)
        if member:
            member.user.session_stale = True
            db.session.delete(member)
            db.session.commit()
        return True

    @validates("license")
    def _validate_license(self, key, value):
        if value not in self.VALID_LICENSE:
            raise ValueError(f"Invalid license: {value}")
        return value

    def get_owner_email(self):
        if not (user := User.query.get(self.owner_id)):
            return "unknown"
        return user.email

    def can_we_invite_user(self, email, user=None):
        if not User.validate_email(email):
            return (False, "Invalid email")

        if self.has_member(user or email):
            return (False, "User already exists in the tenant")

        if not self.approved_domains:
            return (True, None)

        domain = email.rsplit("@", 1)[-1]
        for approved in self.approved_domains.split(","):
            if approved.strip() == domain:
                return (True, None)
        return (False, "User domain is not within the approved domains of the tenant")

    def get_default_roles(self, roles):
        if not roles:
            return ["user"]

        if not isinstance(roles, list):
            roles = [roles]

        if "user" not in roles:
            roles.append("user")
        return roles

    @staticmethod
    def create(user, name, email, approved_domains=None, license="free"):

        tenant = Tenant(
            owner_id=user.id,
            name=name.lower(),
            contact_email=email,
            approved_domains=approved_domains,
            license=license,
        )
        db.session.add(tenant)
        db.session.commit()

        # Add user as Admin to the tenant
        response = tenant.add_member(
            user_or_email=user,
            attributes={"roles": ["admin"]},
            send_notification=False,
        )
        return tenant

    def delete(self):
        db.session.delete(self)
        db.session.commit()
        return True


class Role(db.Model):
    __tablename__ = "roles"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    name = db.Column(db.String(50), nullable=False, server_default="")
    label = db.Column(db.Unicode(255), server_default="")

    VALID_ROLE_NAMES = ["user", "viewer", "editor", "admin"]

    @staticmethod
    def find_by_name(name):
        return Role.query.filter(func.lower(Role.name) == func.lower(name)).first()


class TenantMember(db.Model):
    """
    Represents a user in a specific tenant, with roles assigned.
    """

    __tablename__ = "tenant_members"
    __table_args__ = (db.UniqueConstraint("user_id", "tenant_id"),)
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    user_id = db.Column(db.String, db.ForeignKey("users.id", ondelete="CASCADE"))
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id", ondelete="CASCADE"))
    roles = db.relationship(
        "Role",
        secondary="tenant_member_roles",
        lazy="selectin",
        backref=db.backref("tenant_members", lazy="dynamic"),
    )


class TenantMemberRole(db.Model):
    """
    This table assigns a specific role to a TenantMember (user in a specific tenant).
    """
    __tablename__ = "tenant_member_roles"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_member_id = db.Column(
        db.String, db.ForeignKey("tenant_members.id", ondelete="CASCADE")
    )
    role_id = db.Column(db.String, db.ForeignKey("roles.id", ondelete="CASCADE"))


class User(db.Model, UserMixin):
    __tablename__ = "users"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    is_active = db.Column(db.Boolean(), nullable=False, server_default="1")
    email = db.Column(db.String(255), nullable=False, unique=True)
    email_confirmed_at = db.Column(db.DateTime())
    email_confirm_code = db.Column(
        db.String,
        default=lambda: str(shortuuid.ShortUUID().random(length=6)).lower(),
    )
    password = db.Column(db.String(255), nullable=False, server_default="")
    last_password_change = db.Column(db.DateTime())
    login_count = db.Column(db.Integer, default=0)
    display_name = db.Column(db.String(100), nullable=False, server_default="")
    super = db.Column(db.Boolean(), nullable=False, server_default="0")
    built_in = db.Column(db.Boolean(), default=False)
    has_seen_welcome = db.Column(db.Boolean(), default=False)
    tenant_limit = db.Column(db.Integer, default=1)
    trial_days = db.Column(db.Integer, default=14)
    can_user_create_tenant = db.Column(db.Boolean(), nullable=False, server_default="1")
    license = db.Column(db.String(255), nullable=False, server_default="free")
    session_stale = db.Column(db.Boolean(), nullable=False, server_default="0")
    memberships = db.relationship("TenantMember", backref="user", lazy="dynamic")
    date_added = db.Column(db.DateTime, default=datetime.utcnow)
    date_updated = db.Column(db.DateTime, onupdate=datetime.utcnow)

    VALID_LICENSE = ["free", "silver", "gold"]

    @validates("license")
    def _validate_license(self, key, value):
        if value not in self.VALID_LICENSE:
            raise ValueError(f"Invalid license: {value}")
        return value

    @validates("email")
    def _validate_email(self, key, address):
        if address:
            try:
                email_validator.validate_email(address, check_deliverability=False)
            except:
                abort(422, "Invalid email")
        return address

    def as_dict(self, tenant=None):
        data = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        if tenant:
            data["roles"] = self.roles_for_tenant(tenant)
        else:
            data["tenants"] = [tenant.name for tenant in self.get_tenants()]
        data.pop("password", None)
        data["date_added"] = _fmt_dt(self.date_added)
        data["date_updated"] = _fmt_dt(self.date_updated)
        return data

    @staticmethod
    def validate_registration(email, password, password2):
        if not email:
            abort(500, "Invalid or empty email")
        if not misc.perform_pwd_checks(password, password_two=password2):
            abort(500, "Invalid password")
        if not User.validate_email(email):
            abort(500, "Invalid email")
        if User.find_by_email(email):
            abort(500, "Email already exists")

    @staticmethod
    def validate_email(email):
        if not email:
            return False
        try:
            email_validator.validate_email(email, check_deliverability=False)
        except:
            return False
        return True

    @staticmethod
    def email_to_object(user_or_email, or_404=False):
        if isinstance(user_or_email, User):
            return user_or_email
        if user := User.find_by_email(user_or_email):
            return user
        if or_404:
            abort(404, "User not found")
        return None

    def is_password_change_required(self):
        if not self.last_password_change:
            return True
        return False

    @staticmethod
    def add(
        email,
        password=None,
        display_name=None,
        confirmed=None,
        super=False,
        built_in=False,
        tenants=None,
        license="free",
        is_active=True,
        require_pwd_change=False,
        send_notification=False,
        return_user_object=False,
    ):
        """
        Add user

        tenants: [{"id":1,"roles":["user"]}]
        """
        tenants = tenants or []
        if not password:
            password = uuid4().hex

        User.validate_registration(email, password, password)

        email_confirmed_at = None
        if confirmed:
            email_confirmed_at = datetime.utcnow()

        new_user = User(
            email=email,
            display_name=display_name,
            email_confirmed_at=email_confirmed_at,
            built_in=built_in,
            super=super,
            license=license,
            is_active=is_active,
        )
        new_user.set_password(password, set_pwd_change=not require_pwd_change)
        db.session.add(new_user)
        db.session.commit()
        for record in tenants:
            if tenant := Tenant.query.get(record["id"]):
                tenant.add_member(
                    user_or_email=new_user,
                    attributes={"roles": record.get("roles")},
                    send_notification=False,
                )

        token = User.generate_invite_token(email=new_user.email, expiration=604800)
        link = "{}{}?token={}".format(
            current_app.config["HOST_NAME"], "register", token
        )
        sent_email = False
        if send_notification and current_app.is_email_configured:
            send_template_email(
                "Super Admin",
                recipients=[new_user.email],
                content="You have been added as a super user",
                button_link=link
            )
            sent_email = True

        if return_user_object:
            return new_user

        return {
            "id": new_user.id,
            "success": True,
            "message": f"Added {new_user.email}",
            "access_link": link,
            "sent-email": sent_email,
        }

    def send_email_confirmation(self):
        if self.email_confirmed_at:
            abort(422, "user is already confirmed")
        if not current_app.is_email_configured:
            abort(500, "email is not configured")

        link = "{}{}?code={}".format(
            current_app.config["HOST_NAME"], "register", self.email_confirm_code
        )
        send_template_email(
            "Confirm Email",
            recipients=[self.email],
            content=f"Please enter the following code to confirm your email: {self.email_confirm_code}",
            button_link=link,
            button_label="Login",
        )
        return True

    @staticmethod
    def find_by_email(email):
        if user := User.query.filter(
            func.lower(User.email) == func.lower(email)
        ).first():
            return user
        return None

    @staticmethod
    def verify_auth_token(token):
        data = misc.verify_jwt(token)
        if data is False:
            return False
        return User.query.get(data["id"])

    def generate_auth_token(self, expiration=600):
        data = {"id": self.id}
        return misc.generate_jwt(data, expiration)

    @staticmethod
    def verify_invite_token(token):
        data = misc.verify_jwt(token)
        if data is False:
            return False
        return data

    @staticmethod
    def generate_invite_token(email, tenant_id=None, expiration=600, attributes=None):
        attributes = attributes or {}
        data = {**attributes, **{"email": email}}
        if tenant_id:
            data["tenant_id"] = tenant_id
        return misc.generate_jwt(data, expiration)

    @staticmethod
    def verify_magic_token(token):
        data = misc.verify_jwt(token)
        if data is False:
            return False
        if data.get("type") != "magic_link":
            return False
        return data

    def generate_magic_link(self, tenant_id, expiration=600):
        data = {
            "email": self.email,
            "user_id": self.id,
            "tenant_id": tenant_id,
            "type": "magic_link",
        }
        return misc.generate_jwt(data, expiration)

    def get_tenants(self, own=False):
        if own:
            return Tenant.query.filter(Tenant.owner_id == self.id).all()
        if self.super:
            return Tenant.query.all()

        member_tenant_ids = (
            db.session.query(TenantMember.tenant_id)
            .filter(TenantMember.user_id == self.id)
        )
        return (
            Tenant.query
            .filter(
                db.or_(
                    Tenant.owner_id == self.id,
                    Tenant.id.in_(member_tenant_ids)
                )
            )
            .all()
        )

    def has_tenant(self, tenant):
        return tenant.has_member(self, get_user_object=True)

    def has_role_for_tenant(self, tenant, role_name):
        return tenant.has_member_with_role(self, role_name)

    def has_any_role_for_tenant(self, tenant, role_names):
        if not isinstance(role_names, list):
            role_names = [role_names]
        member = tenant.has_member(self)
        if not member:
            return False
        member_roles = {role.name.lower() for role in member.roles}
        return any(r.lower() in member_roles for r in role_names)

    def has_all_roles_for_tenant(self, tenant, role_names):
        if not isinstance(role_names, list):
            role_names = [role_names]
        member = tenant.has_member(self)
        if not member:
            return False
        member_roles = {role.name.lower() for role in member.roles}
        return all(r.lower() in member_roles for r in role_names)

    def all_roles_by_tenant(self, tenant):
        member = tenant.has_member(self)
        active_role_ids = (
            {role.id for role in member.roles} if member else set()
        )
        return [
            {
                "role_name": role.name,
                "role_id": role.id,
                "enabled": role.id in active_role_ids,
            }
            for role in Role.query.all()
        ]

    def roles_for_tenant(self, tenant):
        return tenant.get_roles_for_member(self)

    def roles_for_tenant_by_id(self, tenant_or_id):
        if isinstance(tenant_or_id, Tenant):
            tenant = tenant_or_id
        else:
            tenant = Tenant.query.get(str(tenant_or_id))
        if not tenant:
            return []
        return self.roles_for_tenant(tenant)

    def set_password(self, password, set_pwd_change=True):
        if not misc.perform_pwd_checks(password, password_two=password):
            abort(422, "Invalid password - failed checks")

        self.password = generate_password_hash(password)
        if set_pwd_change:
            self.last_password_change = datetime.utcnow()

    def check_password(self, password):
        return check_password_hash(self.password, password)

    def set_confirmation(self):
        self.email_confirmed_at = datetime.utcnow()

class Flow(db.Model):
    __tablename__ = "flows"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False, default="Untitled Flow")
    description = db.Column(db.String)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    folder = db.Column(db.String(255), nullable=True)
    is_published = db.Column(db.Boolean, nullable=False, server_default="0")
    notes = db.Column(db.Text, nullable=True)
    user_id = db.Column(db.String, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    nodes = db.relationship("Node", backref="flow", cascade="all, delete-orphan", lazy=True)
    edges = db.relationship("Edge", backref="flow", cascade="all, delete-orphan", lazy=True)
    runs = db.relationship("Run", backref="flow", cascade="all, delete-orphan", lazy=True)

    def to_dict(self):
        return {
            "id": self.id, "tenant_id": self.tenant_id, "name": self.name,
            "description": self.description,
            "folder": self.folder,
            "created_at": self.created_at.isoformat(), "updated_at": self.updated_at.isoformat(),
            "is_published": self.is_published,
        }


class Node(db.Model):
    __tablename__ = "nodes"
    id = db.Column(db.String, primary_key=True, default=lambda: shortuuid.ShortUUID().random(length=16).lower(), unique=True)
    flow_id = db.Column(db.String, db.ForeignKey("flows.id", ondelete="CASCADE"), nullable=False)
    node_id = db.Column(db.String(64), nullable=False)
    name = db.Column(db.String(255), nullable=False, default="Node")
    description = db.Column(db.String(255), nullable=True)
    label = db.Column(db.String(255), nullable=False, default="")
    node_type = db.Column(db.String(64), nullable=False, default="default")
    pos_x = db.Column(db.Float, default=0)
    pos_y = db.Column(db.Float, default=0)
    inputs = db.Column(db.Integer, default=1)
    outputs = db.Column(db.Integer, default=1)
    has_failure_path = db.Column(db.Boolean, default=False)
    action_id = db.Column(db.String(64), nullable=True)
    action_name = db.Column(db.String(255), nullable=True)

    def to_dict(self):
        return {
            "id": self.id, "node_id": self.node_id, "name": self.name, "label": self.label,
            "node_type": self.node_type, "pos_x": self.pos_x, "pos_y": self.pos_y,
            "inputs": self.inputs, "outputs": self.outputs,
            "has_failure_path": self.has_failure_path,
            "action_id": self.action_id, "action_name": self.action_name,
            "description": self.description,
        }


class Edge(db.Model):
    __tablename__ = "edges"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    flow_id = db.Column(db.String, db.ForeignKey("flows.id", ondelete="CASCADE"), nullable=False)
    source_node_id = db.Column(db.String(64), nullable=False)
    source_output = db.Column(db.String(64), nullable=False)
    target_node_id = db.Column(db.String(64), nullable=False)
    target_input = db.Column(db.String(64), nullable=False)

    def to_dict(self):
        return {
            "id": self.id, "source_node_id": self.source_node_id,
            "source_output": self.source_output, "target_node_id": self.target_node_id,
            "target_input": self.target_input,
        }


class NodeConfig(db.Model):
    __tablename__ = "node_configs"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    flow_id = db.Column(db.String, db.ForeignKey("flows.id", ondelete="CASCADE"), nullable=False)
    node_id = db.Column(db.String(64), nullable=False)
    config_json = db.Column(db.Text, nullable=False, default="{}")
    form_json = db.Column(db.Text, nullable=True, default=None)

    __table_args__ = (db.UniqueConstraint("flow_id", "node_id", name="uq_nodeconfig_flow_node"),)

    def to_dict(self):
        return {"node_id": self.node_id, "config_json": self.config_json}


class Credential(db.Model):
    __tablename__ = "credentials"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    name = db.Column(db.String(64), nullable=False)  # unique per tenant, used in {{cred.<name>.<field>}}
    integration = db.Column(db.String(64), nullable=True)  # optional category
    label = db.Column(db.String(255), nullable=False, default="")  # description
    data_enc = db.Column(db.Text, nullable=False, default="{}")
    created_by = db.Column(db.String, db.ForeignKey("users.id", ondelete="SET NULL"), nullable=True, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint("tenant_id", "name", name="uq_credential_tenant_name"),)

    def set_data(self, plain_dict):
        fernet = current_app.config["FERNET"]
        self.data_enc = json.dumps({k: fernet.encrypt(v.encode()).decode() for k, v in plain_dict.items() if v})

    def decrypted(self):
        try:
            fernet = current_app.config["FERNET"]
            return {k: fernet.decrypt(v.encode()).decode() for k, v in json.loads(self.data_enc or "{}").items()}
        except Exception:
            return {}

    def to_dict(self, masked=True):
        data = self.decrypted()
        if masked:
            data = {k: ("•" * 8 + v[-4:] if len(v) > 4 else "••••") for k, v in data.items()}
        return {
            "id": self.id, "tenant_id": self.tenant_id, "name": self.name,
            "integration": self.integration, "label": self.label,
            "data": data, "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class Run(db.Model):
    __tablename__ = "runs"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    flow_id = db.Column(db.String, db.ForeignKey("flows.id", ondelete="CASCADE"), nullable=False)
    status = db.Column(db.String(32), nullable=False, default="pending")
    started_at = db.Column(db.DateTime, nullable=True)
    finished_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    output = db.Column(db.Text, nullable=True)
    error = db.Column(db.Text, nullable=True)
    job_id = db.Column(db.String(255), nullable=True)
    logs = db.relationship("RunLog", backref="run", cascade="all, delete-orphan", lazy=True)

    def to_dict(self, include_logs=False):
        d = {
            "id": self.id, "flow_id": self.flow_id, "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "output": json.loads(self.output) if self.output else None,
            "error": self.error,
        }
        if include_logs:
            d["logs"] = [l.to_dict() for l in self.logs]
        return d


class RunLog(db.Model):
    __tablename__ = "run_logs"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    run_id = db.Column(db.String, db.ForeignKey("runs.id"), nullable=False)
    node_id = db.Column(db.String(64), nullable=False)
    level = db.Column(db.String(16), nullable=False, default="info")
    message = db.Column(db.Text, nullable=False)
    detail = db.Column(db.Text, nullable=True)
    iteration = db.Column(db.Integer, nullable=True)
    iteration_path = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id, "node_id": self.node_id, "level": self.level,
            "message": self.message, "detail": self.detail, "iteration": self.iteration,
            "iteration_path": json.loads(self.iteration_path) if self.iteration_path else None,
            "created_at": self.created_at.isoformat(),
        }


class DataTable(db.Model):
    __tablename__ = "data_tables"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint("tenant_id", "name", name="uq_datatable_tenant_name"),)


class DataRecord(db.Model):
    __tablename__ = "data_records"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    table_name = db.Column(db.String(255), nullable=False, index=True)
    record_key = db.Column(db.String(255), nullable=True, default=lambda: shortuuid.ShortUUID().random(length=12).lower(), index=True)
    data = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (db.Index("ix_datarecord_tenant_table", "tenant_id", "table_name"),)

    def to_dict(self):
        return {
            "id": self.id, "tenant_id": self.tenant_id, "table_name": self.table_name,
            "key": self.record_key, "data": json.loads(self.data),
            "created_at": self.created_at.isoformat(), "updated_at": self.updated_at.isoformat(),
        }


class FormSession(db.Model):
    __tablename__ = "form_sessions"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    token = db.Column(db.String(64), nullable=False, unique=True, index=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    flow_id = db.Column(db.String, db.ForeignKey("flows.id", ondelete="CASCADE"), nullable=False)
    run_id = db.Column(db.String, db.ForeignKey("runs.id", ondelete="CASCADE"), nullable=True)
    status = db.Column(db.String(32), nullable=False, default="active")
    current_node_id = db.Column(db.String(64), nullable=False)
    step_index = db.Column(db.Integer, nullable=False, default=0)
    total_steps = db.Column(db.Integer, nullable=False, default=1)
    data_bus = db.Column(db.Text, nullable=False, default="{}")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def get_data_bus(self): return json.loads(self.data_bus or "{}")
    def set_data_bus(self, data): self.data_bus = json.dumps(data, default=str)

    def to_dict(self):
        return {
            "token": self.token, "tenant_id": self.tenant_id, "flow_id": self.flow_id,
            "status": self.status, "current_node_id": self.current_node_id,
            "step_index": self.step_index, "total_steps": self.total_steps,
            "created_at": self.created_at.isoformat(),
        }


class Ticket(db.Model):
    __tablename__ = "tickets"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    title = db.Column(db.String(500), nullable=False, default="Untitled")
    status = db.Column(db.String(50), nullable=False, default="open")
    priority = db.Column(db.String(50), nullable=False, default="medium")
    assignee = db.Column(db.String(200), nullable=True)
    tags = db.Column(db.JSON, nullable=True, default=list)
    content = db.Column(db.JSON, nullable=True, default=dict)
    meta = db.Column(db.JSON, nullable=True, default=dict)
    flow_id = db.Column(db.String, nullable=True)
    flow_run_id = db.Column(db.String, nullable=True)
    node_id = db.Column(db.String(50), nullable=True)
    closed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def to_dict(self):
        return {
            "id": self.id, "tenant_id": self.tenant_id, "title": self.title,
            "status": self.status, "priority": self.priority, "assignee": self.assignee,
            "tags": self.tags or [], "content": self.content or {}, "meta": self.meta or {},
            "flow_id": self.flow_id, "flow_run_id": self.flow_run_id, "node_id": self.node_id,
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class TicketComment(db.Model):
    __tablename__ = "ticket_comments"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    ticket_id = db.Column(db.String, db.ForeignKey("tickets.id", ondelete="CASCADE"), nullable=False)
    author = db.Column(db.String(200), nullable=False, default="Anonymous")
    body = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def to_dict(self):
        return {"id": self.id, "ticket_id": self.ticket_id, "author": self.author,
                "body": self.body, "created_at": self.created_at.isoformat() if self.created_at else None}


class Dashboard(db.Model):
    __tablename__ = "dashboards"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    tenant_id = db.Column(db.String, db.ForeignKey("tenants.id"), nullable=False, index=True)
    name = db.Column(db.String(500), nullable=False, default="Untitled Dashboard")
    widgets = db.Column(db.JSON, nullable=True, default=list)
    created_at = db.Column(db.DateTime, server_default=db.func.now())
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def to_dict(self):
        return {
            "id": self.id, "tenant_id": self.tenant_id, "name": self.name,
            "widgets": self.widgets or [],
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }

class LogLevel(str, enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @classmethod
    def values(cls) -> list[str]:
        return [m.value for m in cls]

    @classmethod
    def coerce(cls, value: str) -> "LogLevel":
        """Return a valid LogLevel, falling back to INFO for unknown values."""
        try:
            return cls(value.upper())
        except (ValueError, AttributeError):
            return cls.INFO


class LogAction(str, enum.Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    SYSTEM = "SYSTEM"

    @classmethod
    def coerce(cls, value: str) -> "LogAction":
        try:
            return cls(value.upper())
        except (ValueError, AttributeError):
            return cls.GET


class LogNamespace(str, enum.Enum):
    GENERAL = "general"
    SYSTEM = "system"

    @classmethod
    def coerce(cls, value: str) -> str:
        """Namespaces are open-ended; just normalise to lowercase."""
        return (value or cls.GENERAL.value).lower()


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

class Logs(db.Model):
    __tablename__ = "logs"
    id = db.Column(db.String, primary_key=True, default=_short_id, unique=True)
    namespace = db.Column(db.String(), nullable=False, default=LogNamespace.GENERAL.value)
    level     = db.Column(db.String(), nullable=False, default=LogLevel.INFO.value)
    action    = db.Column(db.String(), nullable=False, default=LogAction.GET.value)
    message   = db.Column(db.String(), nullable=False)
    success   = db.Column(db.Boolean(), default=True)
    meta      = db.Column(db.JSON(), default=dict)
    user_id   = db.Column(db.String(), db.ForeignKey("users.id"),   nullable=True)
    tenant_id = db.Column(db.String(), db.ForeignKey("tenants.id"), nullable=True)
    date_added   = db.Column(db.DateTime, default=datetime.utcnow)
    date_updated = db.Column(db.DateTime, onupdate=datetime.utcnow)

    # Eagerly loadable relationships — eliminates N+1 in as_dict()
    user   = db.relationship("User",   foreign_keys=[user_id],   lazy="joined")
    tenant = db.relationship("Tenant", foreign_keys=[tenant_id], lazy="joined")

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def as_dict(self) -> dict:
        """
        Serialize to a dict. Relationships are joined on load, so accessing
        self.user / self.tenant does NOT issue additional queries.
        """
        data = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        data["user_email"]   = self.user.email   if self.user   else "deleted"
        data["tenant_name"]  = self.tenant.name  if self.tenant else "deleted"
        data["date_added"] = _fmt_dt(self.date_added)
        data["date_updated"] = _fmt_dt(self.date_updated)
        return data

    def __str__(self) -> str:
        """Human-readable single-line representation (replaces as_readable)."""
        formatted_date = arrow.get(self.date_added).format("YYYY-MM-DD HH:mm:ss")
        user_str   = f"User:{self.user_id}"   if self.user_id   else "User:N/A"
        tenant_str = f"Tenant:{self.tenant_id}" if self.tenant_id else "Tenant:N/A"
        success_str = "Yes" if self.success else "No"
        return (
            f"[{formatted_date} - {self.level}] | {tenant_str} | {user_str} | "
            f"Action:{self.action} | Success:{success_str} | {self.message}"
        )

    # kept for backwards-compat; delegates to __str__
    def as_readable(self) -> str:
        return str(self)

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------
    @staticmethod
    def add(
        message: str = "unknown",
        action: str = LogAction.GET.value,
        level: str = LogLevel.INFO.value,
        namespace: str = LogNamespace.GENERAL.value,
        success: bool = True,
        user_id: str | None = None,
        tenant_id: str | None = None,
        meta: dict | None = None,
        stdout: bool = False,
    ) -> "Logs":
        """
        Create and persist a log entry.

        Example::

            Logs.add(message="something happened", level="error", action="put")
        """
        coerced_level     = LogLevel.coerce(level)
        coerced_action    = LogAction.coerce(action)
        coerced_namespace = LogNamespace.coerce(namespace)

        entry = Logs(
            namespace=coerced_namespace,
            message=message,
            level=coerced_level.value,
            action=coerced_action.value,
            success=success,
            user_id=user_id,
            tenant_id=tenant_id,
            meta=meta or {},
        )
        db.session.add(entry)
        db.session.commit()

        if stdout:
            logger_fn = getattr(current_app.logger, coerced_level.value.lower())
            logger_fn(
                "Audit: %s | %s | %s | %s | %s | %s",
                tenant_id, user_id, coerced_namespace,
                success, coerced_action.value, message,
            )

        return entry

    @staticmethod
    def add_system_log(**kwargs) -> "Logs":
        """
        Shorthand for system-namespace logs (not tied to a specific tenant).

        Example::

            Logs.add_system_log(message="testing", level="error", action="put")
        """
        return Logs.add(namespace=LogNamespace.SYSTEM.value, **kwargs)

    # ------------------------------------------------------------------
    # Query builder
    # ------------------------------------------------------------------

    @staticmethod
    def _build_query(
        id: str | None = None,
        message: str | None = None,
        action: str | None = None,
        namespace: str | None = None,
        level: str | list[str] | None = None,
        user_id: str | None = None,
        tenant_id: str | None = None,
        success: bool | None = None,
        span: int | None = None,
        meta: dict | None = None,
    ):
        """
        Return a filtered, un-limited SQLAlchemy query.

        Keeping limit/pagination out of this method means as_count works
        correctly (COUNT on an unlimited query, not a COUNT(LIMIT N) subquery).
        """
        q = (
            Logs.query
            .order_by(Logs.date_added.desc())
            # Pull in relationships in one shot so callers never trigger N+1
            .options(
                db.joinedload(Logs.user),
                db.joinedload(Logs.tenant),
            )
        )

        if id:
            q = q.filter(Logs.id == id)
        if message:
            q = q.filter(Logs.message == message)
        if namespace:
            q = q.filter(func.lower(Logs.namespace) == namespace.lower())
        if action:
            q = q.filter(func.lower(Logs.action) == action.upper())
        if success is not None:
            q = q.filter(Logs.success == success)
        if user_id:
            q = q.filter(Logs.user_id == user_id)
        if tenant_id:
            q = q.filter(Logs.tenant_id == tenant_id)
        if level:
            levels = [level] if isinstance(level, str) else level
            q = q.filter(
                func.lower(Logs.level).in_([lvl.lower() for lvl in levels])
            )
        if meta:
            for key, value in meta.items():
                q = q.filter(Logs.meta.op("->>")(key) == value)
        if span:
            cutoff = arrow.utcnow().shift(hours=-span).datetime
            q = q.filter(Logs.date_added >= cutoff)

        return q

    @staticmethod
    def get(
        id: str | None = None,
        message: str | None = None,
        action: str | None = None,
        namespace: str | None = None,
        level: str | list[str] | None = None,
        user_id: str | None = None,
        tenant_id: str | None = None,
        success: bool | None = None,
        span: int | None = None,
        meta: dict | None = None,
        # ---- result-shaping options ----
        limit: int = 100,
        as_query: bool = False,
        as_count: bool = False,
        as_dict: bool = False,
        paginate: bool = False,
        page: int = 1,
        per_page: int = 10,
    ) -> list["Logs"] | dict | int:
        """
        Flexible log retrieval.

        Only one of ``as_query``, ``as_count``, ``as_dict``, or ``paginate``
        should be True at a time; they are checked in that order.

        Example::

            Logs.get(level="error", namespace="my_ns", meta={"key": "value"})
        """
        filter_kwargs = dict(
            id=id, message=message, action=action, namespace=namespace,
            level=level, user_id=user_id, tenant_id=tenant_id,
            success=success, span=span, meta=meta,
        )
        q = Logs._build_query(**filter_kwargs)

        if as_query:
            return q

        # Count must happen BEFORE limit is applied
        if as_count:
            return q.count()

        # Paginate must also happen BEFORE a hard limit
        if paginate:
            return q.paginate(page=page, per_page=per_page)

        # Apply the row cap last
        q = q.limit(limit)

        rows = q.all()
        if as_dict:
            return [row.as_dict() for row in rows]
        return rows

    @staticmethod
    def get_system_log(**kwargs) -> list["Logs"] | dict | int:
        """Shorthand for querying system-namespace logs."""
        return Logs.get(namespace=LogNamespace.SYSTEM.value, **kwargs)

def _fmt_dt(dt):
    return dt.isoformat() + "Z" if dt else None

@login.user_loader
def load_user(user_id):
    return User.query.get(user_id)