# spark-submit  --master local[1]  worker.py
from celery.bin.celery import main

main( [ 'main', '-A', 'tasks', 'worker', '--concurrency', '1' ] )
