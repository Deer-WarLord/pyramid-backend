# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db.models import Sum, F

from factrum_group.serializers import *
from rest_framework import generics
from rest_framework import permissions
from rest_framework_csv import renderers as r

r.CSVRenderer.writer_opts = {
    "delimiter": str(u';')
}


class InfoRenderer(r.CSVRenderer):
    header = ['theme', 'url', 'publication', 'date', 'views']


class AnalyzedInfoList(generics.ListAPIView):
    queryset = AnalyzedInfo.objects.all()
    serializer_class = AnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class AnalyzedInfoDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = AnalyzedInfo.objects.all()
    serializer_class = AnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class SocialDetailsList(generics.ListAPIView):
    queryset = SocialDetails.objects.all()
    serializer_class = SocialDetailsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class SocialDetailsDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = SocialDetails.objects.all()
    serializer_class = SocialDetailsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class ExportAnalyzedInfoGeneral(generics.ListAPIView):
    renderer_classes = (r.CSVRenderer,)
    queryset = AnalyzedInfo.objects
    serializer_class = AnalyzedInfoGeneralSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get(self, request, *args, **kwargs):

        if len(request.query_params):
            self.queryset = AnalyzedInfo.objects.filter(**dict(request.query_params.items())).values(
                theme=F("title__title")).annotate(views=Sum("views")).order_by("-views")
        else:
            self.queryset = AnalyzedInfo.objects.values(
                theme=F("title__title")).annotate(views=Sum("views")).order_by("-views")

        return self.list(request, *args, **kwargs)


class ExportAnalyzedInfoDetails(generics.ListAPIView):
    renderer_classes = (InfoRenderer,)
    queryset = AnalyzedInfo.objects
    serializer_class = AnalyzedInfoDetailSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get(self, request, *args, **kwargs):

        if len(request.query_params):
            self.queryset = AnalyzedInfo.objects.filter(**dict(request.query_params.items())).order_by("-views")
        else:
            self.queryset = AnalyzedInfo.objects.all().order_by("-views")

        return self.list(request, *args, **kwargs)
