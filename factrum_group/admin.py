# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

# Register your models here.

from factrum_group.models import *


@admin.register(AnalyzedInfo)
class AnalyzedInfoAdmin(admin.ModelAdmin):
    list_display = ['get_upload_title', 'get_url', 'get_title', 'views', 'created_date']
    search_fields = ['upload_info__title', 'article__url', 'title__title', 'views', 'article__posted_date',
                     'created_date']
    date_hierarchy = 'article__posted_date'
    list_filter = ['upload_info__title', 'title__title']

    ordering = ['-created_date', 'views', 'article__posted_date', 'created_date']

    def get_upload_title(self, obj):
        return obj.upload_info.title

    get_upload_title.admin_order_field = 'upload_info__title'
    get_upload_title.short_description = 'Period'

    def get_title(self, obj):
        return obj.title.title

    get_title.admin_order_field = 'title__title'
    get_title.short_description = 'Key word'

    def get_url(self, obj):
        return obj.article.url

    get_url.admin_order_field = 'article__url'
    get_url.short_description = 'URL'


def built_publication_rating(modeladmin, request, queryset):
    pass


@admin.register(SocialDetails)
class SocialDetailsAdmin(admin.ModelAdmin):
    list_display = ['get_title', 'views', 'sex_view', 'age', 'education', 'children_lt_16', 'marital_status', 'occupation',
                    'group', 'income', 'region', 'typeNP']
    ordering = ['views']

    def get_title(self, obj):
        return obj.title.title

    get_title.admin_order_field = 'title__title'
    get_title.short_description = 'Key word'

    def sex_view(self, obj):
        for key, value in obj.sex.items():
            return "{0}: {1}".format(key, value)
        return ''

    sex_view.short_description = 'sex'
    actions = [built_publication_rating]


@admin.register(PublicationsSocialDemoRating)
class PublicationsSocialDemoRatingAdmin(admin.ModelAdmin):
    list_display = ['publication', 'views', 'sex', 'age', 'education', 'children_lt_16', 'marital_status', 'occupation',
                    'group', 'income', 'region', 'typeNP']
    ordering = ['views']