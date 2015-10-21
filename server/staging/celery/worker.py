from celery.bin.celery import main

main( [ 'main', '-A', 'staging.celery.manager', 'worker', '--concurrency', '1' ] )

