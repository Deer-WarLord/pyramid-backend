from __future__ import unicode_literals

from django.conf.urls import url
from admixer.views import *

app_name = 'admixer'
urlpatterns = [
    url(r'^analyzed-info/$', AnalyzedInfoList.as_view(), name='analyzed-info'),
    url(r'^analyzed-info/(?P<pk>[0-9]+)/$', AnalyzedInfoDetail.as_view(), name='analyzed-info-edit'),
]