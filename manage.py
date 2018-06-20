import sys
import os

from click import echo
from flask_script import Manager#, MigrateCommand
from bittrex import create_app, db

app = create_app('development')
manager = Manager(create_app)

# manager.add_command('db', MigrateCommand)

@app.cli.command('urlmap')
def urlmap():
    """Prints out all routes"""
    echo("{:50s} {:40s} {}".format('Endpoint', 'Methods', 'Route'))
    for route in app.url_map.iter_rules():
        methods = ','.join(route.methods)
        echo("{:50s} {:40s} {}".format(route.endpoint, methods, route))

@manager.command
def createdb(drop_first=False):
    """Creates the database."""
    if drop_first:
        db.drop_all()
    db.create_all()

if __name__ == "__main__":
    manager.run()