from flask import render_template, session, request, abort
from . import main
from app.models import Node
from app.utils.decorators import login_required
from app.flows.flow_helpers import _enforce_form_auth


@main.route("/")
@login_required
def home():
    return render_template("home.html")

@main.route("/flows")
@login_required
def flows_page():
    return render_template("flows.html")

@main.route("/flows/<flow_id>")
@login_required
def view_flow(flow_id):
    return render_template("view_flow.html", flow_id=flow_id)

@main.route("/tables")
@login_required
def tables():
    return render_template("data_tables.html")

@main.route("/tickets")
@login_required
def tickets():
    return render_template("tickets.html")

@main.route("/tickets/<ticket_id>")
@login_required
def view_ticket(ticket_id):
    return render_template("ticket_view.html", ticket_id=ticket_id)

@main.route("/dashboards")
@login_required
def dashboards():
    return render_template("dashboards.html")

@main.route("/dashboards/<dashboard_id>")
@login_required
def view_dashboard(dashboard_id):
    return render_template("dashboard_view.html", dashboard_id=dashboard_id)

@main.route("/credentials")
@login_required
def credentials():
    return render_template("credentials.html")

@main.route("/form-builder")
@login_required
def form_builder():
    return render_template("form_builder.html")

@main.route("/form/<node_id>")
def public_form(node_id):
    node = Node.query.get(node_id)
    if not node:
        abort(404)
    try:
        _enforce_form_auth(node)
    except Exception as e:
        if "__password_required__" in str(e):
            error = bool(request.args.get("form_password"))
            return render_template("form_password.html", node_id=node_id, error=error)
        raise
    return render_template("form_public.html", flow_id=node.flow_id, node_id=node_id)