from __future__ import absolute_import, unicode_literals
from celery.decorators import task

from admixer.utils import save_admixer_data
from noksfishes.models import ShukachPublication

import logging
logger = logging.getLogger(__name__)


def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]


@task(name="async_get_admixer_data")
def async_get_admixer_data():
    shukach_ids = ShukachPublication.objects.all().values_list("shukach_id", flat=True).distinct()
    total = len(shukach_ids)
    logger.info("Total shuckach ids count: %d" % total)
    current = 0
    for batch_ids in chunks(shukach_ids, 10000):
        save_admixer_data(batch_ids)
        current += len(batch_ids)
        logger.info("Processed: %d/%d" % (current, total))
