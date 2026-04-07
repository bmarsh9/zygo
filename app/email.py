from threading import Thread
from flask import current_app
from flask_mail import Message
from app import mail
import smtplib
import requests


def _send_mailjet(subject, sender, recipients, text_body, html_body):
    """Send email via Mailjet API v3.1."""
    api_key = current_app.config["MAILJET_API_KEY"]
    api_secret = current_app.config["MAILJET_API_SECRET"]

    messages = []
    for recipient in recipients:
        msg = {
            "From": {"Email": sender, "Name": current_app.config.get("APP_NAME")},
            "To": [{"Email": recipient}],
            "Subject": subject,
        }
        if text_body:
            msg["TextPart"] = text_body
        if html_body:
            msg["HTMLPart"] = html_body
        messages.append(msg)

    try:
        resp = requests.post(
            "https://api.mailjet.com/v3.1/send",
            auth=(api_key, api_secret),
            json={"Messages": messages},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        for msg_result in data.get("Messages", []):
            status = msg_result.get("Status")
            to = msg_result.get("To", [{}])[0].get("Email", "unknown")
            if status == "success":
                current_app.logger.info(f"Mailjet: sent '{subject}' to {to}")
            else:
                current_app.logger.warning(
                    f"Mailjet: status '{status}' for '{subject}' to {to} — {msg_result}"
                )

        return data
    except requests.exceptions.HTTPError:
        current_app.logger.error(
            f"Mailjet: HTTP {resp.status_code} for '{subject}' to {recipients} — {resp.text}"
        )
        raise
    except requests.exceptions.RequestException as e:
        current_app.logger.error(
            f"Mailjet: request failed for '{subject}' to {recipients} — {e}"
        )
        raise

def _send_mailjet_async(app, subject, sender, recipients, text_body, html_body):
    with app.app_context():
        try:
            _send_mailjet(subject, sender, recipients, text_body, html_body)
            current_app.logger.debug("Mailjet email sent successfully")
        except Exception as e:
            current_app.logger.error(f"Failed to send Mailjet email: {e}")


def send_async_email(app, msg):
    with app.app_context():
        try:
            mail.send(msg)
            current_app.logger.debug("Email sent successfully")
        except smtplib.SMTPException as e:
            current_app.logger.error(f"Failed to send email: {e}")


def send_email(subject, recipients, text_body, html_body, async_send=True):
    provider = current_app.config.get("EMAIL_PROVIDER")
    sender = current_app.config.get("MAIL_DEFAULT_SENDER") or current_app.config.get("MAIL_USERNAME")

    if not sender:
        current_app.logger.warning("No email sender configured")
        return False

    if provider == "mailjet":
        try:
            if async_send:
                Thread(
                    target=_send_mailjet_async,
                    args=(current_app._get_current_object(), subject, sender, recipients, text_body, html_body)
                ).start()
            else:
                _send_mailjet(subject, sender, recipients, text_body, html_body)
                current_app.logger.debug("Mailjet email sent successfully (sync)")
                return True
        except Exception as e:
            current_app.logger.error(f"Failed to send Mailjet email: {e}")
            return False
    else:
        msg = Message(subject, sender=sender, recipients=recipients)
        msg.body = text_body
        msg.html = html_body
        try:
            if async_send:
                Thread(
                    target=send_async_email,
                    args=(current_app._get_current_object(), msg)
                ).start()
            else:
                mail.send(msg)
                current_app.logger.debug("Email sent successfully (sync)")
                return True
        except smtplib.SMTPException as e:
            current_app.logger.error(f"Failed to send email (sync): {e}")
            return False

def send_template_email(subject, recipients, content, button_link=None, button_label="View", help_link=None, **kwargs):
    """Send an email using the standard basic_template."""
    from flask import render_template
    ctx = dict(title=subject, content=content, button_link=button_link, button_label=button_label, help_link=help_link)
    send_email(
        subject,
        recipients=recipients,
        text_body=render_template("email/basic_template.txt", **ctx),
        html_body=render_template("email/basic_template.html", **ctx),
        **kwargs,
    )