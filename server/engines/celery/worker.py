from celery.bin.celery import main

main( [ 'main', '-A', 'tasks', 'worker', '--concurrency', '1' ] )
