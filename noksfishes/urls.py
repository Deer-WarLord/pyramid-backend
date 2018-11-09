from __future__ import unicode_literals

from django.conf.urls import url
from noksfishes.views import *

app_name = 'noksfishes'
urlpatterns = [
    url(r'^analyzed-info/$', AnalyzedInfoList.as_view(), name='analyzed-info'),
    url(r'^analyzed-info/(?P<pk>[0-9]+)/$', AnalyzedInfoDetail.as_view(), name='analyzed-info-edit'),

    url(r'^publications/$', PublicationsList.as_view(), name='publications'),
    url(r'^publications-title-date/$', PublicationTitleDateList.as_view(), name='publications-title-date'),
    url(r'^publication/(?P<pk>[0-9]+)/$', PublicationsDetail.as_view(), name='publication-edit'),

    url(r'^export-publications/$', ExportPublicationsList.as_view(), name='export-publications'),

    url(r'^export-tv-publications/$', ExportTvPublicationsList.as_view(), name='export-tv-publications'),

    url(r'^publications-without-keys/$', PublicationsWithoutKeysList.as_view(), name='publications-without-keys'),
    url(r'^publications-without-keys-count/$', get_publications_without_keys_count, name='publications-without-keys-count'),
    url(r'^get-keys/$', get_keys, name='get-keys'),
    url(r'^get-keys-from-url/$', get_keys_from_url, name='get-keys-from-url')

]