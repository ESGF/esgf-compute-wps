#! /usr/bin/env python

from django.db.models import Sum
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_http_methods

from . import common

from wps import models

@require_http_methods(['GET'])
@ensure_csrf_cookie
def admin_stats(request):
    try:
        common.authentication_required(request)

        common.authorization_required(request)

        stat_type = request.GET.get('type', None)

        data = {}

        if stat_type == 'files':
            files = data['files'] = []

            files_qs = models.File.objects.annotate(count=Sum('requested'))

            for file_obj in files_qs:
                files.append(file_obj.to_json())
        else:
            processes = data['processes'] = []

            processes_qs = models.Process.objects.all()

            for process_obj in processes_qs:
                processes.append(process_obj.to_json(True))
    except Exception as e:
        return common.failed(e.message)
    else:
        return common.success(data)
