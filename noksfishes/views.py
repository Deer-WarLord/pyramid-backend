# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db.models import Count
from django.http import HttpResponse

from noksfishes.serializers import *
from rest_framework import generics
from rest_framework import permissions
from rest_framework_csv import renderers as r
import csv

from noksfishes.tasks import async_get_ids_for_urls, async_get_ids_for_urls_from_json

r.CSVRenderer.writer_opts = {
    "delimiter": str(u';')
}


class AnalyzedInfoList(generics.ListAPIView):
    queryset = AnalyzedInfo.objects.all()
    serializer_class = AnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class AnalyzedInfoDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = AnalyzedInfo.objects.all()
    serializer_class = AnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class PublicationsList(generics.ListAPIView):
    queryset = Publication.objects.all()
    serializer_class = PublicationSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class PublicationsDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Publication.objects.all()
    serializer_class = PublicationSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class ExportPublicationsList(generics.ListAPIView):
    renderer_classes = (r.CSVRenderer, )
    queryset = Publication.objects
    serializer_class = ExportPublicationSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get(self, request, *args, **kwargs):

        if len(request.query_params):
            self.queryset = Publication.objects.filter(**dict(request.query_params.items()))

        return self.list(request, *args, **kwargs)


class PublicationTitleDateList(generics.ListAPIView):
    queryset = Publication.objects
    serializer_class = PublicationTitleDateSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get(self, request, *args, **kwargs):

        if len(request.query_params):
            self.queryset = Publication.objects.filter(
                **dict(request.query_params.items())
            ).values('title', 'posted_date').annotate(count=Count("title")).order_by("-count")

        return self.list(request, *args, **kwargs)


class ExportTvPublicationsList(generics.ListAPIView):
    renderer_classes = (r.CSVRenderer, )
    queryset = Publication.objects.all()
    serializer_class = ExportTvPublicationSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get(self, request, *args, **kwargs):

        if len(request.query_params):
            self.queryset = Publication.objects.filter(**dict(request.query_params.items()))

        return self.list(request, *args, **kwargs)


class PublicationsWithoutKeysList(generics.ListAPIView):
    queryset = Publication.objects.filter(shukachpublication__isnull=True).exclude(url__exact='').distinct().order_by("-posted_date")
    serializer_class = PublicationSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


def get_publications_without_keys_count(request):
    count = Publication.objects.filter(shukachpublication__isnull=True).exclude(url__exact='').distinct().count()
    return HttpResponse(count)


def get_keys(request):
    async_get_ids_for_urls.delay()
    return HttpResponse()


def get_keys_from_url(request):
    async_get_ids_for_urls_from_json.delay((request.GET["url"], request.GET["title"]))
    return HttpResponse()