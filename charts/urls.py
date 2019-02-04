from __future__ import unicode_literals

from django.conf.urls import url

from charts.views import *
app_name = 'charts'
urlpatterns = [
    url(r'^keyword/$', Keyword.as_view(), name='keyword'),
    url(r'^object/$', Object.as_view(), name='object'),
    url(r'^keyword-fg/$', KeywordFactrumViews.as_view(), name='keyword-fg'),
    url(r'^object-fg/$', ObjectFactrumViews.as_view(), name='object-fg'),
    url(r'^keyword-fg-sd/$', KeywordFactrumSdViews.as_view(), name='keyword-fg-sd'),
    url(r'^keyword-admixer-sd/$', KeywordAdmixerSdViews.as_view(), name='keyword-admixer-sd'),
    url(r'^object-fg-sd/$', ObjectFactrumSdViews.as_view(), name='object-fg-sd'),
    url(r'^object-admixer-sd/$', ObjectAdmixerSdViews.as_view(), name='object-admixer-sd'),
    url(r'^themes/$', ThemeList.as_view(), name='themes'),
    url(r'^objects/$', ObjectsList.as_view(), name='objects')
]