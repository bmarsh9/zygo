from flask import Blueprint, request, session
from flask_login import current_user

api = Blueprint("api", __name__)
internal_api = Blueprint("internal_api", __name__)


@api.after_request
def audit_log(response):
    if request.method not in ("POST", "PUT", "PATCH", "DELETE"):
        return response
    try:
        from app.models import Logs
        success = 200 <= response.status_code < 300
        Logs.add(
            message=f"{request.method} {request.path}",
            action=request.method.lower(),
            level="info" if success else "warning",
            success=success,
            user_id=current_user.id if current_user.is_authenticated else None,
            tenant_id=session.get("tenant_id"),
            meta={
                "email":  current_user.email if current_user.is_authenticated else None,
                "method": request.method,
                "path":   request.path,
                "status": response.status_code,
            }
        )
    except Exception:
        pass
    return response

from . import base, views, internal