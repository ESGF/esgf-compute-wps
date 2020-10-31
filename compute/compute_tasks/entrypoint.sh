#! /bin/bash

celery -A compute_tasks.celery_app worker ${@} 
