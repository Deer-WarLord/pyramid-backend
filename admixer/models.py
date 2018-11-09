# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from uploaders.models import UploadedInfo
from datetime import datetime


class AnalyzedInfo(models.Model):

    upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE,
                                    editable=False, related_name="admixer", blank=True, null=True)
    url_id = models.IntegerField()
    platform = models.IntegerField()
    browser = models.IntegerField()
    region = models.CharField(max_length=256, default="UA", blank=True, null=True)
    age = models.IntegerField()
    gender = models.IntegerField()
    income = models.IntegerField()
    uniques = models.IntegerField()
    views = models.IntegerField()

    class Meta:
        verbose_name = "Analyzed Info"
        verbose_name_plural = "Analyzed Info"

