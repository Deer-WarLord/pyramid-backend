# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import calendar
import datetime

from django.db import models
from django.contrib.auth.models import AbstractUser
from django.contrib.postgres.fields import JSONField
from django.db.models.signals import post_delete
from django.dispatch.dispatcher import receiver
from django.utils.timezone import now
from rest_framework.exceptions import ValidationError


class Provider(models.Model):
    title = models.CharField(max_length=256)
    description = models.CharField(max_length=256)
    parser_info = JSONField()

    def __unicode__(self):
        return self.title


class User(AbstractUser):
    provider = models.ForeignKey(Provider, on_delete=models.CASCADE, blank=True, null=True)

    def __unicode__(self):
        return self.username


class UploadedInfo(models.Model):
    provider = models.ForeignKey(Provider, on_delete=models.CASCADE)
    title = models.CharField(max_length=50)
    file = models.FileField()
    created_date = models.DateTimeField(default=now)

    def is_in_period(self, start_date, end_date, period):

        if period == "week":
            try:
                start_period = datetime.datetime.strptime(self.title, "%w-%W-%Y")
                end_period = start_period + datetime.timedelta(days=6)
            except ValueError:
                return False
        else:
            try:
                start_period = datetime.datetime.strptime(self.title, "%m-%Y")
                end_period = start_period + datetime.timedelta(calendar.monthrange(start_period.year, start_period.month)[1])
            except ValueError:
                return False

        return start_period >= start_date and end_period <= end_date

    def __unicode__(self):
        return self.title


@receiver(post_delete, sender=UploadedInfo)
def remove_file_from_folder(sender, instance, **kwargs):
    instance.file.delete(False)
