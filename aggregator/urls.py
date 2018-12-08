from __future__ import unicode_literals

from django.conf.urls import url

from aggregator.views import *

app_name = 'aggregator'
urlpatterns = [
    url(r'^general/$', FactrumAdmixerGeneralInfoList.as_view(), name='general'),
    url(r'^social-details/$', FactrumAdmixerSocialDetailsList.as_view(), name='social-details'),
    url(r'^market-rating/$', MarketsRating.as_view(), name='market-rating'),
    url(r'^theme-company-rating/$', ThemeCompanyRating.as_view(), name='theme-company-rating'),
    url(r'^region-rating/$', RegionRating.as_view(), name='region-rating'),
    url(r'^publication-type-rating/$', PublicationTypeRating.as_view(), name='publication-type-rating'),
    url(r'^publication-topic-rating/$', PublicationTopicRating.as_view(), name='publication-topic-rating'),
    url(r'^publication-rating/$', PublicationRating.as_view(), name='publication-rating'),
    url(r'^specific-social-demo-rating-admixer/$', SpecificSocialDemoRatingAdmixer.as_view(),
        name='specific-social-demo-rating-admixer'),
    url(r'^general-social-demo-rating-admixer/$', GeneralSocialDemoRatingAdmixer.as_view(),
        name='general-social-demo-rating-admixer'),
    url(r'^special-by-theme-social-demo-rating-fg/$', SpecialByThemeSocialDemoRatingFG.as_view(),
        name='special-by-theme-social-demo-rating-fg'),
    url(r'^special-by-theme-publication-social-demo-rating-fg/$', SpecialByThemePublicationSocialDemoRatingFG.as_view(),
        name='special-by-theme-publication-social-demo-rating-fg'),
    url(r'^general-by-themes-social-demo-rating-fg/$', GeneralByThemesSocialDemoRatingFG.as_view(),
        name='general-by-themes-social-demo-rating-fg'),
    url(r'^general-by-publications-social-demo-rating-fg/$', GeneralByPublicationsSocialDemoRatingFG.as_view(),
        name='general-by-publications-social-demo-rating-fg')
]