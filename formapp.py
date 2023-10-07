from flask_wtf import FlaskForm
from wtforms import SubmitField,RadioField,DateField,SelectField,StringField,TextAreaField
from wtforms.validators import InputRequired,DataRequired


class FormHttpReq(FlaskForm):
    sdpenv = SelectField('SDP ENVIRONTMENT',
                          choices=[('10.64.30.95:9480', 'SIT'), ('192.168.86.208:9003', 'PROD')],
                          validators=[DataRequired()])
    username = StringField('SYSTEM ID',validators=[DataRequired()])
    passw = StringField('PASSWORD',validators=[DataRequired()])
    sdc = StringField('SHORTCODE',validators=[DataRequired()])
    msisdn = StringField('MSISDN',validators=[DataRequired()])
    msg= TextAreaField('MESSAGE',validators=[DataRequired()])
    submit=SubmitField('Submit')

class FormLog(FlaskForm):
    trxid = StringField('transaction id',validators=[DataRequired()])
    dt=DateField('DATE SEND',validators=[DataRequired()])
    logtype = RadioField('Environment', choices=[('sdp','SDP'),
                                                               ('scp','SCP')])
    submit=SubmitField('Submit')

class FormCdr(FlaskForm):
    msisdn = StringField('MSISDN',validators=[DataRequired()])
    dates=DateField('DATE SEND',validators=[DataRequired()])
    hours = SelectField(u'HOUR ',
                          choices=[('00', '00'), ('01', '01'),('02', '02'),('03', '03'),('04', '04'),
                                   ('05', '05'), ('06', '06'),('07', '07'),('08', '08'),('09', '09'),
                                   ('10', '10'), ('11', '11'),('11', '11'),('12', '12'),('13', '13'),
                                   ('14', '14'), ('15', '15'),('16', '16'),('17', '17'),('18', '18'),
                                   ('19', '19'), ('20', '20'),('21', '21'),('22', '22'),('23', '23')])
    cdrtype = RadioField('Environment', choices=[('sdp','SDP'),
                                                               ('scp','SCP')])
    submit=SubmitField('Submit')


class FormCpId(FlaskForm):
        cpid= StringField('transaction id',validators=[DataRequired()])
        submit=SubmitField('Submit')



