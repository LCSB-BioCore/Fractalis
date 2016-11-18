from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired


class POSTAnalyticsForm(FlaskForm):
    task = StringField('task', validators=[DataRequired()])
