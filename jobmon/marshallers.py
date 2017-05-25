from flask_restplus import fields


batch = {
    'batch_id': fields.Integer,
    'name': fields.String,
    'user': fields.String
}

job = {
    'jid': fields.Integer,
    'batch_id': fields.Integer,
    'name': fields.String,
    'runfile': fields.String,
    'args': fields.String,
    'submitted_date': fields.DateTime
}

job_instance = {
    'job_instance_id': fields.Integer,
    'jid': fields.Integer,
    'job_instance_type': fields.String,
    'retry_number': fields.Integer,
    'usage_str': fields.String,
    'wallclock': fields.String,
    'current_status': fields.Integer,
    'cpu': fields.String,
    'io': fields.String,
    'submitted_date': fields.DateTime
}

job_instance_error = {
    'id': fields.Integer,
    'job_instance_id': fields.Integer,
    'error_time': fields.DateTime,
    'description': fields.String
}

job_instance_status = {
    'id': fields.Integer,
    'job_instance_id': fields.Integer,
    'status_time': fields.DateTime,
    'status': fields.Integer
}

status = {
    'id': fields.Integer,
    'label': fields.String
}
