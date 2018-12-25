from collections import Counter

from clickhouse_driver import Client
from django.db.models import Count, Sum
from django.db.models.functions import ExtractWeek, ExtractYear
from django.conf import settings
from rest_framework import generics, permissions
from rest_framework.response import Response

from admixer.serializers import DynamicAnalyzedInfoSerializer
from aggregator.permissions import IsRequestsToThemeAllow
from charts.serializers import ThemeCompanyRatingSerializer, ThemeCompanyViewsSerializer, ThemeCompanySdViewsSerializer
from noksfishes.models import Publication
import json
import datetime
from uploaders.models import UploadedInfo

import logging
logger = logging.getLogger(__name__)


def handle_request_params(request):
    params = dict(request.query_params.items())

    if not request.user.has_perm('global_permissions.free_time'):
        params["posted_date__lte"] = "2018-12-30"
        params["posted_date__gte"] = "2018-07-01"
    if "key_word__in" in params:
        params["key_word__in"] = json.loads(params.pop("key_word__in"))
        if not len(params["key_word__in"]):
            params.pop("key_word__in")
    return params


class Keyword(generics.ListAPIView):
    queryset = Publication.objects
    serializer_class = ThemeCompanyRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" not in params:
            params["posted_date__lte"] = "2018-06-01"
            params["posted_date__gte"] = "2018-04-01"

        if "key_word__in" in params:
            self.queryset = Publication.objects.filter(
                **params).values('key_word', year=ExtractYear('posted_date'), week=ExtractWeek('posted_date')).annotate(
                publication_amount=Count("key_word"))

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(data=list(queryset), many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class KeywordFactrumViews(generics.ListAPIView):
    serializer_class = ThemeCompanyViewsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" in params:
            start_date = datetime.datetime.strptime(params.pop("posted_date__gte"), "%Y-%m-%d")
            end_date = datetime.datetime.strptime(params.pop("posted_date__lte"), "%Y-%m-%d")
        else:
            end_date = datetime.datetime.strptime("2018-06-01", "%Y-%m-%d")
            start_date = datetime.datetime.strptime("2018-04-01", "%Y-%m-%d")

        self.queryset = []

        for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
            if not info.is_in_period(start_date, end_date):
                # TODO make flag for week or month
                continue
            self.queryset += info.factrum_group_detailed.filter(
                title__title__in=params["key_word__in"]
            ).values("title__title").annotate(views=Sum("views")).values(
                "views", "title__title", "upload_info__title")

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(data=list(queryset), many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class KeywordFactrumSdViews(generics.ListAPIView):
    serializer_class = ThemeCompanySdViewsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" in params:
            start_date = datetime.datetime.strptime(params.pop("posted_date__gte"), "%Y-%m-%d")
            end_date = datetime.datetime.strptime(params.pop("posted_date__lte"), "%Y-%m-%d")
        else:
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=120)

        if "key_word__in" not in params:
            top = Publication.objects.values('key_word').annotate(
                publication_amount=Count("key_word")).order_by("-publication_amount").values_list(
                "key_word", flat=True)[0]

            params["key_word__in"] = [top]

        self.queryset = []

        display_fields = ["views", "title__title", "upload_info__title"]

        if "sd" in params:
            display_fields.append(params['sd'])

        for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
            if not info.is_in_period(start_date, end_date):
                continue
            self.queryset += info.factrum_group_detailed.filter(
                title__title__in=params["key_word__in"]
            ).values(*display_fields)

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(data=list(queryset), many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class KeywordAdmixerSdViews(generics.ListAPIView):
    serializer_class = DynamicAnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    admixer_values_list = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views', 'date')

    def _chunks(self, l, n):
        return [l[i:i+n] for i in range(0, len(l), n)]

    def _convert(self, tup):
        di = {}
        for a, b in tup:
            di.setdefault(a, []).append(b)
        return di

    def _with_keys(self, d, keys):
        return {x: d[x] for x in d if x in keys}

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        try:
            end_date = params.pop("posted_date__lte")
            start_date = params.pop("posted_date__gte")
        except KeyError as e:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.datetime.now() - datetime.timedelta(days=120)).strftime("%Y-%m-%d")

        if "key_word__in" not in params:
            top = Publication.objects.values('key_word').annotate(
                publication_amount=Count("key_word")).order_by("-publication_amount").values_list(
                "key_word", flat=True)[0]

            params["key_word__in"] = [top]

        display_fields = ['uniques', 'views', 'date']
        sd = params.pop('sd') if "sd" in params else None
        publications = Publication.objects.filter(**params).values_list("key_word", "shukachpublication__shukach_id")
        logger.info("Get %d publications" % len(publications))

        l_part = self._convert(publications)
        logger.info("Convert %d publications" % len(publications))

        self._client = Client(settings.CLICKHOUSE_HOST,
                        database=settings.CLICKHOUSE_DB,
                        user=settings.CLICKHOUSE_USER,
                        password=settings.CLICKHOUSE_PASSWORD)

        total = len(publications)
        current = 0
        self.queryset = []

        for key_word, ids in l_part.items():
            results = {}
            for batch_ids in self._chunks(ids, 10000):
                logger.info("Sent %d ids" % len(batch_ids))
                results = self._query_admixer_data(results, batch_ids, display_fields, sd, start_date, end_date)
                current += len(batch_ids)
                logger.info("Processed: %d/%d" % (current, total))
            for date, items in results.items():
                row = {
                    "key_word": key_word,
                    "views": items["views"],
                    "uniques": items["uniques"],
                    "date": date,
                }
                if sd:
                    row[sd] = dict(Counter(items[sd]))
                self.queryset.append(row)

        self._client.disconnect()

        return self.list(request, *args, **kwargs)

    def _query_admixer_data(self, results, batch_ids, display_fields, sd, start_date, end_date):
        ids = ",".join("'%s'" % item for item in batch_ids)

        query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views), Date ' \
                'from admixer.UrlStat ' \
                'where UrlId in (%s) and Date >= \'%s\' and Date <= \'%s\' ' \
                'Group by UrlId, Platform, Browser, Country, Age, Gender, Income, Date' % (ids, start_date, end_date)

        response = self._client.execute(query)
        keys = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views', 'date')
        df = display_fields + [sd] if sd else display_fields
        for row in response:
            item = dict(zip(keys, row[1:]))
            item = self._with_keys(item, df)
            date = item["date"] - datetime.timedelta(days=item["date"].weekday())
            if date not in results:
                results[date] = item
                if sd:
                    results[date][sd] = [item[sd]]
            else:
                for key in display_fields[:2]:
                    results[date][key] += item[key]
                if sd:
                    results[date][sd].append(item[sd])

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        return Response(queryset)


class ThemeList(generics.ListAPIView):
    queryset = Publication.objects.exclude(market__name__exact = None).values("market__name", "key_word").distinct()
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def list(self, request, *args, **kwargs):
        groups = {}
        for item in self.get_queryset():
            groups.setdefault(item['market__name'], []).append(item["key_word"])

        return Response([{"market": k, "keywords": v} for k, v in groups.items()])
