# -*- coding: utf-8 -*-
# Generated by Django 1.11.25 on 2020-10-20 19:29
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('compute_wps', '0003_remove_auth'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='auth',
            name='user',
        ),
        migrations.DeleteModel(
            name='Auth',
        ),
    ]
