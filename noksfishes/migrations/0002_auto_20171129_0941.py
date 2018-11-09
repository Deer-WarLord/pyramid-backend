# -*- coding: utf-8 -*-
# Generated by Django 1.11.1 on 2017-11-29 09:41
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('noksfishes', '0001_initial'),
        ('uploaders', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='publication',
            name='upload_info',
            field=models.ForeignKey(blank=True, editable=False, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='noksfishes_publications', to='uploaders.UploadedInfo', verbose_name='\u041a\u043b\u044e\u0447 \u0438\u043c\u043f\u043e\u0440\u0442\u0430'),
        ),
        migrations.AddField(
            model_name='analyzedinfo',
            name='upload_info',
            field=models.ForeignKey(editable=False, on_delete=django.db.models.deletion.CASCADE, related_name='noksfishes', to='uploaders.UploadedInfo', verbose_name='\u041a\u043b\u044e\u0447 \u0438\u043c\u043f\u043e\u0440\u0442\u0430'),
        ),
    ]
