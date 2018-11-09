from __future__ import unicode_literals

from django.conf.urls import url

from charts.views import *
app_name = 'charts'
urlpatterns = [
    url(r'^keyword/$', Keyword.as_view(), name='keyword'),
    url(r'^keyword-fg/$', KeywordFactrumViews.as_view(), name='keyword-fg'),
    url(r'^keyword-fg-sd/$', KeywordFactrumSdViews.as_view(), name='keyword-fg-sd'),
    url(r'^keyword-admixer-sd/$', KeywordAdmixerSdViews.as_view(), name='keyword-admixer-sd'),
    url(r'^themes/$', ThemeList.as_view(), name='themes')
]