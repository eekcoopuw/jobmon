from flask import Flask
from flask_migrate import Migrate
from flask_restplus import Resource, Api

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

from jobmon import marshallers
from jobmon import models
from jobmon.models import db

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://docker:docker@db/docker'

api = Api(app)
db.init_app(app)
migrate = Migrate(app, db)


batch_model = api.model('Batch', marshallers.batch)
job_model = api.model('Job', marshallers.job)
ji_model = api.model('JobInstance', marshallers.job_instance)
ji_err_model = api.model('JobInstanceError', marshallers.job_instance_error)
ji_stat_model = api.model('JobInstanceStatus', marshallers.job_instance_status)
st_model = api.model('Status', marshallers.status)


@api.route('/status')
class StatusList(Resource):
    @api.marshal_list_with(st_model)
    def get(self):
        return models.Status.query.all()


@api.route('/batch')
class BatchList(Resource):
    @api.marshal_with(batch_model)
    def get(self):
        return models.Batch.query.all()

    @api.marshal_with(batch_model)
    def post(self):
        batch = models.Batch(**api.payload)
        db.session.add(batch)
        db.session.commit()
        return batch, 201


@api.route('/batch/<int:batch_id>')
class Batch(Resource):
    @api.marshal_with(batch_model)
    def get(self, batch_id):
        return models.Batch.query.filter_by(batch_id=batch_id).first()


@api.route('/batch/<int:batch_id>/job')
class JobList(Resource):
    @api.marshal_with(job_model)
    def get(self, batch_id):
        return models.Job.query.join(models.Batch).filter_by(
            batch_id=batch_id).all()

    @api.marshal_with(job_model)
    def post(self, batch_id):
        job = models.Job(batch_id=batch_id, **api.payload)
        db.session.add(job)
        db.session.commit()
        return job, 201


@api.route('/batch/<int:batch_id>/job/<int:job_id>')
class Job(Resource):
    @api.marshal_with(job_model)
    def get(self, batch_id, job_id):
        return models.Job.query.join(models.Batch).filter(
            batch_id == batch_id,
            models.Job.jid == job_id).all()


@api.route('/batch/<int:batch_id>/job/<int:job_id>/job_instance')
class JobInstanceList(Resource):
    @api.marshal_with(ji_model)
    def get(self, job_id, **kwargs):
        return models.JobInstance.query.join(models.Job).filter_by(
            jid=job_id).all()

    @api.marshal_with(ji_model)
    def post(self, job_id, **kwargs):
        job_inst = models.JobInstance(jid=job_id, **api.payload)
        db.session.add(job_inst)
        db.session.commit()
        return job_inst, 201


@api.route('/batch/<int:batch_id>/job/<int:job_id>/job_instance/'
           '<int:job_instance_id>')
class JobInstance(Resource):
    @api.marshal_with(ji_model)
    def get(self, job_instance_id, **kwargs):
        return models.JobInstance.query.filter_by(
            job_instance_id=job_instance_id).all()


@api.route('/batch/<int:batch_id>/job/<int:job_id>/job_instance/'
           '<int:job_instance_id>/error')
class ErrorList(Resource):
    @api.marshal_with(ji_err_model)
    def get(self, job_instance_id, **kwargs):
        errors = models.JobInstanceError.query.join(
            models.JobInstance).filter_by(
                job_instance_id=job_instance_id).all()
        return errors

    @api.marshal_with(ji_err_model)
    def post(self, job_instance_id, **kwargs):
        error = models.JobInstanceError(job_instance_id=job_instance_id,
                                        **api.payload)
        db.session.add(error)
        db.session.commit()
        return error, 201


@api.route('/batch/<int:batch_id>/job/<int:job_id>/job_instance/'
           '<int:job_instance_id>/error/<int:error_id>')
class Error(Resource):
    @api.marshal_with(ji_model)
    def get(self, error_id, **kwargs):
        return models.JobInstanceError.query.filter_by(
            id=error_id).first()


if __name__ == '__main__':
    app.run(debug=True)
