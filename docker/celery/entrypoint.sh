#! /bin/bash

celery worker -A compute_tasks ${@} 
