# -*- coding: utf-8 -*-
# Generated by Django 1.11.1 on 2018-06-06 16:00
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('noksfishes', '0010_auto_20180606_1556'),
    ]

    operations = [
        migrations.AlterField(
            model_name='publication',
            name='source',
            field=models.CharField(blank=True, max_length=2048, null=True, verbose_name='Первоисточник'),
        ),
        migrations.AlterField(
            model_name='publication',
            name='title',
            field=models.CharField(blank=True, max_length=2048, null=True, verbose_name='Заголовок'),
        ),
    ]
