# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

# Register your models here.

from admixer.models import *
from noksfishes.models import ShukachPublication


@admin.register(AnalyzedInfo)
class AnalyzedInfoAdmin(admin.ModelAdmin):
    list_display = ['url_id', 'platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views']
    search_fields = ['url_id', 'platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views']
    list_filter = ['browser', 'age', 'gender']
    ordering = ['url_id', 'platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views']

    def get_date(self, obj):
        return ShukachPublication.objects.filter(shukach_id=obj.url_id).first().publication.posted_date

    # get_date.admin_order_field = 'article__url'
    # get_date.short_description = 'Posted date'
