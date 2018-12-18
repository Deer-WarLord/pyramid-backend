# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from import_export import resources
from daterange_filter.filter import DateRangeFilter
from import_export.admin import ImportExportModelAdmin
from import_export.fields import Field

from noksfishes.models import *
from admixer.tasks import async_get_admixer_data

from import_export.formats import base_formats


class SCSV(base_formats.CSV):

    def get_title(self):
        return "scsv"

    def export_data(self, dataset, **kwargs):
        kwargs['delimiter'] = ';'
        return self.get_format().export_set(dataset, **kwargs)


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


class PublicationsResource(resources.ModelResource):
    id_article = Field(attribute='id', column_name='id_article')
    id_title = Field()
    title = Field(attribute='key_word', column_name='title')
    site = Field(attribute='title', column_name='site')

    class Meta:
        model = Publication
        fields = ['url', 'posted_date']
        export_order = ('id_article', 'id_title', 'posted_date', 'site', 'title', 'url')
        widgets = {
            'posted_date': {'format': '%Y-%m-%d'},
        }

    def dehydrate_id_title(self, obj):
        return Theme.objects.get_or_create(title=obj.key_word)[0].id


@admin.register(Publication)
class PublicationsAdmin(ImportExportModelAdmin):
    resource_class = PublicationsResource
    formats = (SCSV,)
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


class TvPublications(Publication):
    class Meta:
        proxy = True


class TvPublicationsResource(resources.ModelResource):
    id_article = Field(attribute='id', column_name='id_article')
    id_title = Field()
    posted_time = Field(attribute='inserted_date', column_name='posted_time')
    end_time = Field(attribute='posted_time', column_name='end_time')


    class Meta:
        model = Publication
        fields = ['key_word', 'title', 'posted_date', 'publication']
        export_order = ('id_article', 'id_title', 'key_word', 'title', 'posted_date', 'posted_time', 'end_time', 'publication')
        widgets = {
            'inserted_date': {'format': '%H:%M:%S'},
            'posted_time': {'format': '%H:%M:%S'},
            'posted_date': {'format': '%Y-%m-%d'},
        }

    def dehydrate_id_title(self, obj):
        return Theme.objects.get_or_create(title=obj.key_word)[0].id


@admin.register(TvPublications)
class TvPublicationsAdmin(ImportExportModelAdmin):
    resource_class = TvPublicationsResource
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

    def get_import_formats(self):
        return (SCSV,)

    def get_queryset(self, request):
        return self.model.objects.filter(type="ТВ")


@admin.register(ShukachPublication)
class ShukachPublicationAdmin(admin.ModelAdmin):
    list_display = ['shukach_id']
    # list_display = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'shukach_id']
    search_fields = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'shukach_id']
    ordering = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'shukach_id']
    list_filter = [('publication__posted_date', DateRangeFilter), 'publication__key_word']
    date_hierarchy = 'publication__posted_date'
    actions = [get_admixer_data]


@admin.register(AdeptPublication)
class AdeptPublicationAdmin(admin.ModelAdmin):
    list_display = ['adept_id']
    search_fields = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'adept_id']
    ordering = ['publication__id', 'publication__key_word', 'publication__title', 'publication__url', 'adept_id']
    list_filter = [('publication__posted_date', DateRangeFilter), 'publication__key_word']
    date_hierarchy = 'publication__posted_date'
