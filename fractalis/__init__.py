from flask import Flask
from fractalis.config import configure_app

app = Flask(__name__)
configure_app(app)

