# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from admixer.serializers import *
from rest_framework import generics
from rest_framework import permissions


class AnalyzedInfoList(generics.ListAPIView):
    queryset = AnalyzedInfo.objects.all()
    serializer_class = AnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)


class AnalyzedInfoDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = AnalyzedInfo.objects.all()
    serializer_class = AnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
