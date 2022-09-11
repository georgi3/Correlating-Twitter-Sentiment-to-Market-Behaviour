from supporting_scripts.serialization import serialization_routine
from apscheduler.schedulers.background import BackgroundScheduler
import datetime


if __name__ == 'manage_serialization':
    print(f'Running {__name__}...')
    scheduler = BackgroundScheduler(timezone='US/Eastern')
    scheduler.add_job(serialization_routine, 'interval', minutes=10, next_run_time=datetime.datetime.now())
    scheduler.start()
