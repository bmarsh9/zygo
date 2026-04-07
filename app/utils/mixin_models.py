from functools import partial
from app.utils.authorizer import Authorizer
from sqlalchemy import func
import arrow


class DateMixin(object):
    __table_args__ = {"extend_existing": True}

    def humanize_date(self, date):
        return arrow.get(date).humanize()

    def simple_date(self, date):
        return arrow.get(date).format("MM/DD/YYYY")


class QueryMixin(object):
    __table_args__ = {"extend_existing": True}

    @classmethod
    def get_or_404(cls, id):
        return cls.query.filter(cls.id == str(id)).first_or_404()

    @classmethod
    def find_by(cls, field, value, tenant_id=None, not_found=False):
        """
        Usage:
            User.find_by("email", "test@example.com")
        """
        _query = cls.query.filter(func.lower(getattr(cls, field)) == func.lower(value))
        if tenant_id:
            _query.filter(getattr(cls, "tenant_id") == tenant_id)

        if not_found:
            return _query.first_or_404()

        return _query.first()


class AuthorizerMixin(object):
    __table_args__ = {"extend_existing": True}

    """
    Define authorizer on fields in the SQLAlchemy models:
        # info={"authorizer": {"update": Authorizer.can_user_manage_platform}}
    
    Run Authorizer
        # tenant = Tenant.query.first()
        # user = User.query.first()
        # response = tenant.get_authorizer_decision(user=user, field="id", action="update")
        # print(response)        
    """

    def get_authorize_fields(self, field=None):
        data = {}
        for col in self.__table__.c:
            if not (authorize_data := col.info.get("authorizer")):
                continue
            data[col.key] = authorize_data
        if field:
            return data.get(field)
        return data

    def get_authorizer_decision(self, user, field, action):
        response = self.get_authorize_fields(field=field)

        # The field in the SQL model does not have an 'authorizer' annotation
        if not response:
            return {
                "ok": False,
                "message": f"Authorizer undefined for:{field}",
                "code": 401,
            }

        base_authorizer_action = response.get(action)

        # The field does not have the specific action defined
        if not base_authorizer_action:
            return {
                "ok": False,
                "message": f"Authorizer action:{action} undefined for:{field}",
                "code": 401,
            }

        base_authorizer = Authorizer(user)
        return partial(base_authorizer_action, base_authorizer)()
