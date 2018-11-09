from django.conf import settings
from clickhouse_driver import Client
from admixer.serializers import AnalyzedInfoSerializer
import logging

logger = logging.getLogger(__name__)


def get_analyzed_info(shukach_ids):
    if len(shukach_ids) == 0:
        return []

    client = Client(settings.CLICKHOUSE_HOST,
                    database=settings.CLICKHOUSE_DB,
                    user=settings.CLICKHOUSE_USER,
                    password=settings.CLICKHOUSE_PASSWORD)
    query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views) ' \
            'from admixer.UrlStat ' \
            'where UrlId in (%s) ' \
            'Group by UrlId, Platform, Browser, Country, Age, Gender, Income' % (",".join("'%d'" % item for item in shukach_ids))
    response = client.execute(query)
    keys = ('url_id', 'platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views')
    results = []
    for row in response:
        item = dict(zip(keys, row))
        item['url_id'] = int(row[0])
        results.append(item)
    return results


def save_admixer_data(shukach_ids):
    logger.info("Starting an update of %d shukach ids", len(shukach_ids))
    results = get_analyzed_info(shukach_ids)
    logger.info("Received %d records from ClickHouse", len(results))
    serializer = AnalyzedInfoSerializer(data=results, many=True)
    serializer.is_valid(raise_exception=True)
    serializer.save()
