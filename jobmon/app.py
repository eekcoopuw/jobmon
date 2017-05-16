from flask import Flask
from flask_migrate import Migrate
from flask_restplus import Resource, fields, Api

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

from jobmon import models
from jobmon.models import db

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://docker:docker@db/docker'

api = Api(app)
db.init_app(app)
migrate = Migrate(app, db)


model = api.model('JobInstance', {
    'jid': fields.Integer,
    'job_instance_type': fields.String,
    'current_status': fields.Integer
})


# conn_def = ("mysql://tomflem:bunnyfoot@internal-db-p02.ihme.washington.edu"
#             "/jobmon")
# eng = create_engine(conn_def)
# Session = sessionmaker()
# Session.configure(bind=eng, autocommit=False)


@api.route('/job_instance/<int:job_instance_id>')
class JobInstance(Resource):
    @api.marshal_with(model)
    def get(self, job_instance_id):
        job_instance = models.JobInstance.query.filter_by(
            job_instance_id=job_instance_id)
        return job_instance.all()


if __name__ == '__main__':
    app.run(debug=True)
