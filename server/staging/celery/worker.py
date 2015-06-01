from celery.bin.celery import main

main( [ 'main', '-A', 'engines.spark.tasks', 'worker', '--concurrency', '1' ] )

