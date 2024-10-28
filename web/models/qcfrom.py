"""
QC form model.
"""

from flask_wtf import FlaskForm
import wtforms


class QcForm(FlaskForm):
    """
    QC form class.

    Fields:
        comments (TextAreaField): QC comments.
        user_id (StringField): User ID.
    """

    issues_found = wtforms.SelectMultipleField(
        "Issues Found",
        choices=[
            ("speakers_reversed", "Speaker Reversed"),
            ("wrong_orientation", "Video has wrong orientation"),
            ("poor_lighting", "Poor Lighting"),
            ("poor_audio", "Poor Audio"),
            ("flickering_video", "Flickering Video"),
            ("mask", "Subject / Interviewer with Mask"),
            ("other", "Other (Described in comments)"),
        ],
        description="Select all that apply.",
    )
    no_issues = wtforms.BooleanField(
        "No issues found",
        description="Only check if no issues were found.",
    )
    comments = wtforms.TextAreaField(
        "QC Comments",
        validators=[wtforms.validators.Optional()],
        description="Optional. Please provide any comments.",
    )
    user_id = wtforms.StringField(
        "User ID",
        validators=[wtforms.validators.DataRequired()],
        description="Please use your REDCap ID.",
    )
    submit = wtforms.SubmitField("Submit")
