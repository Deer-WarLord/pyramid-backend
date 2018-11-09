# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from tv.models import TvMetrics


@admin.register(TvMetrics)
class TvMetricsAdmin(admin.ModelAdmin):
    list_display = ["rat", "shr", "get_key_word", "get_title", "get_posted_date",
                    "get_posted_time", "get_end_time", "get_publication"]
    search_fields = ["publication__key_word", "publication__title", "publication__posted_date",
                     "publication__posted_time", "publication__end_time", "publication__publication"]

    def get_key_word(self, obj):
        return obj.publication.key_word

    get_key_word.short_description = 'key_word'

    def get_title(self, obj):
        return obj.publication.title

    get_title.short_description = 'title'

    def get_posted_date(self, obj):
        return obj.publication.posted_date

    get_posted_date.short_description = 'posted_date'

    def get_posted_time(self, obj):
        return obj.publication.posted_time

    get_posted_time.short_description = 'posted_time'

    def get_end_time(self, obj):
        return obj.publication.end_time

    get_end_time.short_description = 'end_time'

    def get_publication(self, obj):
        return obj.publication.publication

    get_publication.short_description = 'publication'
