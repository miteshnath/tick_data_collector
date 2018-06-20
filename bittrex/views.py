from datetime import datetime, timedelta

from flask import Blueprint, jsonify, url_for
from redbeat.schedulers import RedBeatSchedulerEntry

import celery as cly

from bittrex import celery
from bittrex.tasks import tick_task
import time
import bittrex
from bittrex import db
from bittrex.models import oneMinTick
from bittrex.tasks import tick_task

home = Blueprint('home', __name__)


@home.route('/create/ticker/minlater')
def one_min_task():
    """add a new task and start running it after 60 seconds"""
    eta = datetime.utcnow() + timedelta(seconds=60)
    task = tick_task.apply_async(eta=eta)
    return jsonify({
        '_links': {
            'task': url_for('home.taskstatus', task_id=task.id, _external=True)
        }
    }), 202


@home.route('/create/ticker/periodic')
def periodic_ticker():
    """add a new periodic ticker task running every minute"""
    interval = cly.schedules.schedule(run_every=60)  # seconds
    entry = RedBeatSchedulerEntry('one_min_ticker:' + str(time.time()), 'bittrex.tasks.tick_task', interval, args=[], app=celery)
    entry.save()
    # import pdb; pdb.set_trace()
    return jsonify({
        "task_name": entry.name
    }), 202    
   

@home.route('/status/<task_id>/', methods=['GET', 'POST'])
def taskstatus(task_id):
    task = tick_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        # job did not start yet
        response = {
            'state': task.state,
            'status': 'Pending...'
        }
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'current': task.info.get('current', 0),
            'total': task.info.get('total', 1),
            'status': task.info.get('status', '')
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # something went wrong in the background job
        response = {
            'state': task.state,
            'current': 1,
            'total': 1,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)
