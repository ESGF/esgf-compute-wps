# -*- coding: utf-8 -*-
# Generated by Django 1.11.25 on 2020-10-20 19:46
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('compute_wps', '0004_remove_auth'),
    ]

    operations = [
        migrations.CreateModel(
            name='Nonce',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('state', models.CharField(max_length=128)),
                ('redirect_uri', models.CharField(max_length=255)),
                ('next', models.CharField(max_length=255)),
            ],
        ),
    ]
