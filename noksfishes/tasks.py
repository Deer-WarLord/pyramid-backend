from __future__ import absolute_import, unicode_literals

from zipfile import ZipFile, is_zipfile

from celery.decorators import task
from django.db import transaction
from io import BytesIO
import requests
from django.core.files import File
import json

from os.path import splitext

from noksfishes.models import ShukachPublication, Publication
from noksfishes.utils import get_ids_for_urls

import logging
logger = logging.getLogger(__name__)


@task(name="async_get_ids_for_urls")
def async_get_ids_for_urls():
    with transaction.atomic():
        get_ids_for_urls()


@task(name="async_get_ids_for_urls_from_json")
def async_get_ids_for_urls_from_json(data):
    response = requests.get(data[0])
    logger.info("Upload file from %s" % data[0])
    fname = data[0].rsplit('/', 1)[1]
    file = File(BytesIO(response.content), name=fname)

    if is_zipfile(file):
        with ZipFile(file) as zf:
            raw_data = zf.read(splitext(fname)[0])
    else:
        file.seek(0)
        raw_data = file.read()

    json_data = json.loads(raw_data)
    batch = []
    logger.info("Collect batch from %d records" % len(json_data))
    inverted_pub_urls = {}

    for item in Publication.objects.filter(upload_info__title=data[1],
                                           shukachpublication__isnull=True
                                           ).exclude(url__exact='').distinct().values("id", "url"):

        inverted_pub_urls[item["url"]] = inverted_pub_urls.get(item["url"], [])
        inverted_pub_urls[item["url"]].append(item["id"])

    i = 1
    processed_ids = set()
    for row in json_data:
        if row["url"]:
            for publication_id in inverted_pub_urls[row["url"]]:
                if publication_id not in processed_ids:
                    batch.append(ShukachPublication(publication_id=publication_id, shukach_id=row["shukach_id"]))
                    processed_ids.add(publication_id)

        if i % 100 == 0:
            logger.info("%d - rows processed" % i)
        i += 1

    with transaction.atomic():
        objs = ShukachPublication.objects.bulk_create(batch)
        logger.info("Created objects from shukach urls - %d", len(objs))

