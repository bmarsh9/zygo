from flask import current_app, abort
from app import models
from itsdangerous import (
    TimedJSONWebSignatureSerializer as Serializer,
    BadSignature,
    SignatureExpired,
)
import re


class Response:
    def __init__(self, message: str, success: bool):
        self.message = message
        self.success = success

    def __repr__(self):
        return f"Response(message='{self.message}', success={self.success})"


def get_class_by_tablename(table):
    """Return class reference mapped to table.
    :use: current_app.db_tables["users"]
    'User' -> User
    """
    tables = {}
    for c in dir(models):
        if c == table:
            return getattr(models, c)


def perform_pwd_checks(password, password_two=None):
    if not password:
        return False
    if password_two:
        if password != password_two:
            return False
    if len(password) < 12:
        return False
    return True


def verify_jwt(token):
    if not token:
        current_app.logger.warning("Empty token when verifying JWT")
        return False
    s = Serializer(current_app.config["SECRET_KEY"])
    try:
        data = s.loads(token)
    except SignatureExpired:
        current_app.logger.warning("SignatureExpired while verifying JWT")
        return False
    except BadSignature:
        current_app.logger.warning("BadSignature while verifying JWT")
        return False
    return data


def generate_jwt(data={}, expiration=6000):
    s = Serializer(current_app.config["SECRET_KEY"], expires_in=expiration)
    return s.dumps(data).decode("utf-8")


def request_to_json(request):
    data = {
        "headers": dict(request.headers),
        "body": request.get_json(silent=True),
        "args": request.args.to_dict(),
    }
    for property in ["origin", "method", "mimetype", "referrer", "remote_addr", "url"]:
        data[property] = getattr(request, property)
    return data


def get_users_from_text(text, resolve_users=False, tenant=None):
    """
    Given text with emails (@admin@example.com) in it, this function
    will return a list of found emails (or resolved user objects)
    """
    data = []
    emails = re.findall("[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
    if not resolve_users:
        return emails
    for email in emails:
        if user := models.User.find_by_email(email):
            data.append(user)
    return data

