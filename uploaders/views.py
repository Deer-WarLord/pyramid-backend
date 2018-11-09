# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import transaction
from rest_framework.response import Response

from uploaders.serializers import *
from rest_framework import generics, status
from rest_framework import permissions
from rest_framework.parsers import MultiPartParser

from uploaders.tasks import async_save_data_from_provider
from uploaders.utils import RoleViewSetMixin, save_data_from_provider


class UploadedInfoList(RoleViewSetMixin, generics.ListCreateAPIView):
    queryset = UploadedInfo.objects.all().order_by("-created_date")
    serializer_class = UploadedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
    parser_classes = (MultiPartParser,)

    @transaction.atomic
    def perform_create_default(self, serializer):
        upload_info = serializer.save(provider=self.request.user.provider)
        save_data_from_provider(upload_info, self.request.data['file'].json)

    @transaction.atomic
    def perform_create_for_owners(self, serializer):
        save_data_from_provider(serializer.save(), self.request.data['file'].json)

    def get_serializer_class_for_owners(self):
        serializer = UploadedInfoSerializerOwner
        if Provider.objects.get(id=self.request.data["provider"]).title == "tv_rate":
            serializer = UploadedInfoTvSerializer
        return serializer


class AsyncUploadedInfo(generics.GenericAPIView):
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
    parser_classes = (MultiPartParser,)

    def post(self, request, format=None):
        serializer = AsyncUploadedInfoSerializerOwner(data=request.data)
        if serializer.is_valid(raise_exception=True):
            async_save_data_from_provider.delay(serializer.validated_data)
            return Response(serializer.data, status=status.HTTP_201_CREATED)


class UploadedInfoDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = UploadedInfo.objects.all()
    serializer_class = UploadedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)
