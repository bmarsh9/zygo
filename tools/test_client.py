import sys

sys.path.append("..")  # Adds higher directory to python modules path.
from app import create_app
from app.models import *
from app.email import _send_mailjet
import os

app = create_app(os.getenv("FLASK_CONFIG") or "default")


def test():
    with app.app_context():
        app.config["MAILJET_API_KEY"] = "4692cd4cacdc2c59711adf1fb7cfb9b9"
        app.config["MAILJET_API_SECRET"] = "711b1b5f626c7ebe2c0515e14173dfcb"

        r = _send_mailjet("test", "tomkeene91@gmail.com", ["bmarshall735@gmail.com"], "testing", "testing")
        print(r)



test()
