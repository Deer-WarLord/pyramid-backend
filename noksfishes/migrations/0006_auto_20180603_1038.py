# -*- coding: utf-8 -*-
# Generated by Django 1.11.1 on 2018-06-03 10:38
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('noksfishes', '0005_auto_20180603_1031'),
    ]

    operations = [
        migrations.AlterField(
            model_name='publication',
            name='posted_date',
            field=models.DateField(blank=True, null=True, verbose_name='Дата выхода'),
        ),
    ]
