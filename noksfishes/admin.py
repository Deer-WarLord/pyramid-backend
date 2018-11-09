# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from daterange_filter.filter import DateRangeFilter

from noksfishes.models import *
from admixer.tasks import async_get_admixer_data


@admin.register(AnalyzedInfo)
class AnalyzedInfoAdmin(admin.ModelAdmin):
    list_display = ['upload_info', 'title', 'code', 'url', 'site', 'posted_date', 'created_date']
    search_fields = ['upload_info__title', 'title', 'code', 'url', 'site', 'posted_date', 'created_date']
    date_hierarchy = 'posted_date'
    list_filter = ['upload_info__title']
    ordering = ['-created_date', 'upload_info', 'title', 'code', 'url', 'site', 'posted_date']


@admin.register(Category)
class CategoryAdmin(admin.ModelAdmin):
    list_display = ['id', 'title']
    search_fields = ['id', 'title']
    ordering = ['id', 'title']


@admin.register(Theme)
class ThemeAdmin(admin.ModelAdmin):
    list_display = ['id', 'title']
    search_fields = ['id', 'title']
    ordering = ['id', 'title']


@admin.register(Market)
class MarketAdmin(admin.ModelAdmin):
    list_display = ['id', 'name']
    search_fields = ['id', 'name']
    ordering = ['id', 'name']


@admin.register(Publication)
class PublicationsAdmin(admin.ModelAdmin):
    list_display = ["key_word", "title", "inserted_date", "posted_date", "posted_time",
                    "category", "url", "priority", "advertisement", "size", "symbols", "publication", "source",
                    "country", "region", "city", "regionality", "type", "topic", "number", "printing", "page",
                    "fill_rate", "user", "author_tone", "event_tone", "general_tone", "objectivity", "mention_type",
                    "eventivity", "subject", "plot", "author", "top_managers", "companies", "heading", "w", "y", "z",
                    "fc", "r", "ce", "f", "r_small", "s", "e", "ta", "ts", "tc", "tmk", "tmc", "l", "d", "ktl", "kt",
                    "kl", "m", "pg", "h", "result", "periodicity", "activity", "marginality", "cost", "visitors",
                    "citation_index", "width", "k1", "k2", "k3", "dtek_kof", "created_date", "edit_date", "note"]

    search_fields = ['upload_info__title', 'key_word', 'url', 'category', 'country', 'region', 'city', 'type', 'topic']
    date_hierarchy = 'posted_date'
    list_filter = [('posted_date', DateRangeFilter), 'upload_info__title', 'key_word', 'category', 'country', 'region',
                   'city', 'type', 'topic']
    ordering = ["w", "y", "z", "fc", "r", "ce", "f", "r_small", "s", "e", "ta", "ts", "tc", "tmk", "tmc", "l", "d",
                "ktl", "kt", "kl", "m", "pg", "h", "result", "periodicity", "activity", "marginality", "cost",
                "visitors", "citation_index", "width", "k1", "k2", "k3", "dtek_kof"]


def get_admixer_data(modeladmin, request, queryset):
    async_get_admixer_data.delay()


@admin.register(ShukachPublication)
class ShukachPublicationAdmin(admin.ModelAdmin):
    list_display = ['shukach_id']
    # list_display = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'shukach_id']
    search_fields = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'shukach_id']
    ordering = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'shukach_id']
    list_filter = [('publication__posted_date', DateRangeFilter), 'publication__key_word']
    date_hierarchy = 'publication__posted_date'
    actions = [get_admixer_data]
