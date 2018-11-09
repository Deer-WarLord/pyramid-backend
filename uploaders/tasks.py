from __future__ import absolute_import, unicode_literals

from celery.decorators import task
from io import BytesIO
import requests
from django.core.files import File
from django.db import transaction

from uploaders.models import Provider
from uploaders.serializers import UploadedInfoSerializerOwner, UploadedInfoTvSerializer
from uploaders.utils import save_data_from_provider

import logging
logger = logging.getLogger(__name__)


@task(name="async_save_data_from_provider")
def async_save_data_from_provider(data):
    response = requests.get(data["url"])
    logger.info("Upload file from %s" % data["url"])
    fname = data["url"].rsplit('/', 1)[1]

    uploaded_serializer = UploadedInfoSerializerOwner
    if Provider.objects.get(id=data["provider"]).title == "tv_rate":
        uploaded_serializer = UploadedInfoTvSerializer

    serializer = uploaded_serializer(data={
        "title": data["title"],
        "file": File(BytesIO(response.content), name=fname),
        "provider": data["provider"]
    })

    with transaction.atomic():
        serializer.is_valid(raise_exception=True)
        upload_info = serializer.save()
        logger.info("UploadInfo saved for %s" % fname)
        save_data_from_provider(upload_info, serializer.validated_data["file"].json)

