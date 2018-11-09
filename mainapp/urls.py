from django.conf import settings
from django.conf.urls import url, include
from django.contrib import admin
from django.contrib.auth import views as auth_views

from aggregator.views import HomeView
from uploaders.views import *


urlpatterns = [
    url(r'^$', HomeView.as_view(), name='home'),

    url(r'^login/$', auth_views.LoginView.as_view(template_name='login.html'), name='login'),
    url(r'^logout/$', auth_views.LogoutView.as_view(next_page='/login/'), name='logout'),

    url(r'^admin/', admin.site.urls),

    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),

    url(r'^archive/$', UploadedInfoList.as_view(), name='archive'),
    url(r'^archive-async/$', AsyncUploadedInfo.as_view(), name='archive-async'),
    url(r'^archive/(?P<pk>[0-9]+)/$', UploadedInfoDetail.as_view(), name='archive-edit'),

    url(r'^admixer/', include('admixer.urls', namespace='admixer')),

    url(r'^factrum-group/', include('factrum_group.urls', namespace='factrum_group')),

    url(r'^noksfishes/', include('noksfishes.urls', namespace='noksfishes')),

    url(r'^aggregator/', include('aggregator.urls', namespace='aggregator')),

    url(r'^charts/', include('charts.urls', namespace='charts')),
]

if settings.DEBUG:
    import debug_toolbar
    from django.conf.urls.static import static
    import os
    PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
    urlpatterns = [
                      url(r'^__debug__/', include(debug_toolbar.urls)),
                  ] + urlpatterns \
                    + static(r'^static/(?P<path>.*)$', document_root=os.path.join(PROJECT_ROOT, 'static'))\
                    + static(r'^media/(?P<path>.*)$', document_root=os.path.join(PROJECT_ROOT, 'media'))

