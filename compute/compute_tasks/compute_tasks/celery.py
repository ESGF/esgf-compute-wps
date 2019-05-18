from celery import Celery

app = Celery('compute_tasks')

app.conf.event_serializer = 'cwt_json'
app.conf.result_serializer = 'cwt_json'
app.conf.task_serializer = 'cwt_json'

app.autodiscover_tasks(['compute_tasks'], related_name='cdat', force=True)
app.autodiscover_tasks(['compute_tasks'], related_name='job', force=True)
app.autodiscover_tasks(['compute_tasks'], related_name='metrics_', force=True)
