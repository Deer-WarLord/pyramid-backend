from __future__ import unicode_literals

from django.conf.urls import url
from factrum_group.views import *

app_name = 'factrum_group'
urlpatterns = [
    url(r'^analyzed-info/$', AnalyzedInfoList.as_view(), name='analyzed-info'),
    url(r'^analyzed-info/(?P<pk>[0-9]+)/$', AnalyzedInfoDetail.as_view(), name='analyzed-info-edit'),
    url(r'^social-details/$', SocialDetailsList.as_view(), name='social-details'),
    url(r'^social-details/(?P<pk>[0-9]+)/$', SocialDetailsDetail.as_view(), name='social-details-edit'),
    url(r'^export-general-report/$', ExportAnalyzedInfoGeneral.as_view(), name='export-general-report'),
    url(r'^export-detail-report/$', ExportAnalyzedInfoDetails.as_view(), name='export-detail-report'),
]