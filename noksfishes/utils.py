from django.conf import settings
import requests
import json
from noksfishes.serializers import ShukachResponseSerializer
from noksfishes.models import Publication, ShukachPublication, Theme

import logging

logger = logging.getLogger(__name__)


def get_shukach_ids(urls_list):
    r = requests.post(settings.SHUKACH_API_ENDPOINT, data={
        "key": settings.SHUKACH_API_KEY,
        "json_url": json.dumps(urls_list)
    })

    serializer = ShukachResponseSerializer(data=r.json())
    serializer.is_valid(raise_exception=True)
    return serializer.validated_data["data"]


def get_ids_for_urls():
    publications = Publication.objects.filter(
        shukachpublication__isnull=True).exclude(url__exact='').distinct().values("id", "url")

    logger.info("Publications that needs shukach id - %d", len(publications))

    inverted_pub_urls = {}
    for item in publications:
        inverted_pub_urls[item["url"]] = inverted_pub_urls.get(item["url"], [])
        inverted_pub_urls[item["url"]].append(item["id"])

    logger.info("Unique urls for shukach - %d", len(inverted_pub_urls))

    shukach_urls = get_shukach_ids(list(inverted_pub_urls.keys()))

    logger.info("Received urls from shukach - %d", len(shukach_urls))

    batch = []
    processed_ids = set()
    for id, url in shukach_urls.items():
        if url in inverted_pub_urls:
            for publication_id in inverted_pub_urls[url]:
                if publication_id not in processed_ids:
                    batch.append(ShukachPublication(publication_id=publication_id, shukach_id=id))
                    processed_ids.add(publication_id)
        else:
            logger.error("Received nonexisted url from shukach API: %s", url)

    objs = ShukachPublication.objects.bulk_create(batch)

    logger.info("Created objects from shukach urls - %d", len(objs))

    return len(objs)
