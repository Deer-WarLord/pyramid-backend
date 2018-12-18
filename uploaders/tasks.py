from __future__ import absolute_import, unicode_literals

from ftplib import FTP

from celery.decorators import task
from io import BytesIO
import requests
from django.core.files import File
from django.db import transaction
from django.conf import settings
from django.core import mail

from noksfishes.admin import SCSV
from noksfishes.admin import PublicationsResource
from noksfishes.models import Publication
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
    provider_name = Provider.objects.get(id=data["provider"]).title
    if provider_name == "tv_rate":
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
    if data["send_to_provider"] == "on" and provider_name == "shuckach_publication":
        with FTP(host=settings.FACTUM_HOST) as ftp:
            ftp.login(user=settings.FACTUM_USER, passwd=settings.FACTUM_PASSWORD)
            ftp.cwd("SourceData")
            logger.info("Connected to Factum FTP")
            data_set = PublicationsResource().export(Publication.objects.filter(upload_info__title=upload_info.title))
            with open("/tmp/%s.csv" % upload_info.title, "w") as text_file:
                text_file.write(SCSV().export_data(data_set))
            logger.info("Temporary saved %s.csv" % upload_info.title)
            with open("/tmp/%s.csv" % upload_info.title, "rb") as binary_file:
                logger.info("Open saved %s.csv via FTP" % upload_info.title)
                ftp.set_pasv(False)
                ftp.storbinary('STOR %s.csv' % upload_info.title, binary_file)
                logger.info("Send saved %s.csv via FTP" % upload_info.title)
                mail.send_mail('Автоматическое письмо. Pyramid. Загрузка данных',
                               'Файл %s.csv успешно загружен на сервре Factum' % upload_info.title,
                               'pyramid@gmail.com', ['deerwarlord@gmail.com', 'olepole2009@gmail.com'], fail_silently=False)
