# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

from noksfishes.models import Publication
from uploaders.models import UploadedInfo
from django.contrib.postgres.fields import JSONField


# class TvChannelInfo(models.Model):
#     upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE, editable=False, related_name="tv")
#     publication = models.ForeignKey(Publication, on_delete=models.CASCADE, related_name="tv_publication",
#                                     null=True, blank=True)
#     posted_date = models.DateTimeField()
#     channel = models.CharField(max_length=255, verbose_name="Канал")
#     genre = models.CharField(max_length=255, verbose_name="Жанр")
#     media_group = models.CharField(max_length=255, verbose_name="Медиа группа")
#     time_rate = JSONField(blank=True, null=True, verbose_name="Рейтинг по времени")


class TvMetrics(models.Model):
    upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE, editable=False, related_name="tv_short")
    publication = models.OneToOneField(Publication, on_delete=models.CASCADE, related_name="tv_publication_short",
                                    null=True, blank=True)
    rat = models.FloatField(default=0.0)
    shr = models.FloatField(default=0.0)