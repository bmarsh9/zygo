from threading import Thread
from flask import current_app
from flask_mail import Message
from app import mail
import smtplib


def send_async_email(app, msg):
    with app.app_context():
        try:
            mail.send(msg)
            current_app.logger.debug("Email sent successfully")
        except smtplib.SMTPException as e:
            current_app.logger.error(f"Failed to send email: {e}")


def send_email(subject, recipients, text_body, html_body, async_send=True):
    sender = current_app.config.get("MAIL_DEFAULT_SENDER") or current_app.config.get("MAIL_USERNAME")

    if not sender:
        current_app.logger.warning("No email sender configured")
        return False

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