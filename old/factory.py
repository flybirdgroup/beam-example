import flask
from old.utils import load_configuration
from old.beam import dataflow_bp


def create_app(cfg=None):

    app = flask.Flask(__name__)

    if cfg:
        app.config.from_object(cfg)
    else:
        app.config.from_object(load_configuration())

    app.register_blueprint(dataflow_bp, url_prefix='/dataflow')

    return app
