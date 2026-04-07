from threading import Thread
from flask import current_app
from flask_mail import Message
from app import mail
import smtplib
import requests


def _send_sender(subject, sender, recipients, text_body, html_body):
    """Send email via Sender.net API v2."""
    api_token = current_app.config["SENDER_API_TOKEN"]
    app_name = current_app.config.get("APP_NAME", "")

    results = []
    for recipient in recipients:
        payload = {
            "from": {"email": sender, "name": app_name},
            "to": {"email": recipient},
            "subject": subject,
        }
        if html_body:
            payload["html"] = html_body
        if text_body:
            payload["text"] = text_body

        try:
            resp = requests.post(
                "https://api.sender.net/v2/message/send",
                headers={
                    "Authorization": f"Bearer {api_token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                json=payload,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            current_app.logger.info(f"Sender: sent '{subject}' to {recipient}")
            results.append(data)
        except requests.exceptions.HTTPError:
            current_app.logger.error(
                f"Sender: HTTP {resp.status_code} for '{subject}' to {recipient} — {resp.text}"
            )
            raise
        except requests.exceptions.RequestException as e:
            current_app.logger.error(
                f"Sender: request failed for '{subject}' to {recipient} — {e}"
            )
            raise

    return results


def _send_sender_async(app, subject, sender, recipients, text_body, html_body):
    with app.app_context():
        try:
            _send_sender(subject, sender, recipients, text_body, html_body)
            current_app.logger.debug("Sender email sent successfully")
        except Exception as e:
            current_app.logger.error(f"Failed to send Sender email: {e}")


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

    if provider == "sender":
        try:
            if async_send:
                Thread(
                    target=_send_sender_async,
                    args=(current_app._get_current_object(), subject, sender, recipients, text_body, html_body)
                ).start()
            else:
                _send_sender(subject, sender, recipients, text_body, html_body)
                current_app.logger.debug("Sender email sent successfully (sync)")
                return True
        except Exception as e:
            current_app.logger.error(f"Failed to send Sender email: {e}")
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