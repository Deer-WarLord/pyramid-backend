# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from uploaders.models import UploadedInfo
from django.contrib.postgres.fields import JSONField
from noksfishes.models import Publication, Theme
from django.utils.timezone import now


class AnalyzedInfo(models.Model):
    upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE, editable=False,
                                    related_name="factrum_group")
    article = models.ForeignKey(Publication, on_delete=models.CASCADE)
    title = models.ForeignKey(Theme, on_delete=models.CASCADE)
    views = models.PositiveIntegerField()
    created_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Analyzed Info"
        verbose_name_plural = "Analyzed Info"


class SocialDetails(models.Model):
    upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE, editable=False,
                                    related_name="factrum_group_detailed")
    title = models.ForeignKey(Theme, on_delete=models.CASCADE)
    views = models.IntegerField()
    sex = JSONField(blank=True, null=True)
    age = JSONField(blank=True, null=True)
    education = JSONField(blank=True, null=True)
    children_lt_16 = JSONField(blank=True, null=True)
    marital_status = JSONField(blank=True, null=True)
    occupation = JSONField(blank=True, null=True)
    group = JSONField(blank=True, null=True)
    income = JSONField(blank=True, null=True)
    region = JSONField(blank=True, null=True)
    typeNP = JSONField(blank=True, null=True)


class PublicationsSocialDemoRating(models.Model):
    publication = models.CharField(max_length=1024)
    views = models.IntegerField()
    sex = JSONField(blank=True, null=True)
    age = JSONField(blank=True, null=True)
    education = JSONField(blank=True, null=True)
    children_lt_16 = JSONField(blank=True, null=True)
    marital_status = JSONField(blank=True, null=True)
    occupation = JSONField(blank=True, null=True)
    group = JSONField(blank=True, null=True)
    income = JSONField(blank=True, null=True)
    region = JSONField(blank=True, null=True)
    typeNP = JSONField(blank=True, null=True)
    created_date = models.DateTimeField(default=now)
