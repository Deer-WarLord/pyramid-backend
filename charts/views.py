from collections import Counter
from itertools import groupby

from clickhouse_driver import Client
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count, Sum
from django.db.models.functions import ExtractWeek, ExtractYear
from django.conf import settings
from rest_framework import generics, permissions
from rest_framework.response import Response

from admixer.serializers import DynamicAnalyzedInfoSerializer
from aggregator.permissions import IsRequestsToThemeAllow
from charts.serializers import *
from factrum_group.serializers import SocialDetailsSerializer
from noksfishes.models import Publication
import json
import copy
import datetime
from uploaders.models import UploadedInfo

import logging

logger = logging.getLogger(__name__)


def handle_request_params(request):
    params = dict(request.query_params.items())

    if not request.user.has_perm('global_permissions.free_time'):
        params["posted_date__lte"] = settings.DEFAULT_TO_DATE
        params["posted_date__gte"] = settings.DEFAULT_FROM_DATE
    if "key_word__in" in params:
        params["key_word__in"] = json.loads(params.pop("key_word__in"))
        if not len(params["key_word__in"]):
            params.pop("key_word__in")
    if "object__in" in params:
        params["object__in"] = json.loads(params.pop("object__in"))
        if not len(params["object__in"]):
            params.pop("object__in")

    return params


class Keyword(generics.ListAPIView):
    queryset = Publication.objects
    serializer_class = ThemeCompanyRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" not in params:
            params["posted_date__lte"] = settings.DEFAULT_TO_DATE
            params["posted_date__gte"] = settings.DEFAULT_FROM_DATE

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


class Object(generics.ListAPIView):
    queryset = Publication.objects
    serializer_class = ObjectCompanyRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" not in params:
            params["posted_date__lte"] = settings.DEFAULT_TO_DATE
            params["posted_date__gte"] = settings.DEFAULT_FROM_DATE

        if "object__in" in params:
            self.queryset = Publication.objects.filter(
                **params).values('object', year=ExtractYear('posted_date'), week=ExtractWeek('posted_date')).annotate(
                publication_amount=Count("object"))

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
            end_date = datetime.datetime.strptime(settings.DEFAULT_TO_DATE, "%Y-%m-%d")
            start_date = datetime.datetime.strptime(settings.DEFAULT_FROM_DATE, "%Y-%m-%d")

        self.queryset = []

        for info in UploadedInfo.objects.filter(provider__title="factrum_group"):
            if not info.is_in_period(start_date, end_date, "week"):
                continue
            self.queryset += info.factrum_group.filter(
                title__title__in=params["key_word__in"]
            ).values("title__title").annotate(views=Sum("views")).values(
                "views", "title__title", "upload_info__title", "upload_info__created_date")

        f_title = lambda o: o['upload_info__title']
        f_date = lambda o: o["upload_info__created_date"]
        self.queryset = [max(items, key=f_date) for g, items in groupby(sorted(self.queryset, key=f_title), key=f_title)]

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(data=list(queryset), many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class KeywordAdmixerViews(generics.ListAPIView):
    serializer_class = ThemeCompanyViewsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def _chunks(self, l, n):
        return [l[i:i + n] for i in range(0, len(l), n)]

    def _convert(self, tup):
        di = {}
        for a, b in tup:
            di.setdefault(a, []).append(b)
        return di

    def _query_admixer_data(self, results, batch_ids, start_date, end_date):
        ids = ",".join("'%s'" % item for item in batch_ids)

        query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views), Date ' \
                'from admixer.UrlStat ' \
                'where UrlId in (%s) and Date >= \'%s\' and Date <= \'%s\' ' \
                'Group by UrlId, Platform, Browser, Country, Age, Gender, Income, Date' % (ids, start_date, end_date)

        response = self._client.execute(query)
        keys = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views', 'date')
        for row in response:
            item = dict(zip(keys, row[1:]))
            date = item["date"] - datetime.timedelta(days=item["date"].weekday())
            if date not in results:
                results[date] = item["views"]
            else:
                results[date] += item["views"]

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" not in params:
            end_date = settings.DEFAULT_TO_DATE
            start_date = settings.DEFAULT_FROM_DATE
        else:
            end_date = params["posted_date__lte"]
            start_date = params["posted_date__gte"]

        self.queryset = []

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
                results = self._query_admixer_data(results, batch_ids, start_date, end_date)
                current += len(batch_ids)
                logger.info("Processed: %d/%d" % (current, total))

            for date, views in results.items():
                self.queryset.append({
                    "key_word": key_word,
                    "views": views,
                    "date": date,
                })

        self._client.disconnect()

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        return Response(queryset)


class ObjectFactrumViews(generics.ListAPIView):
    serializer_class = ObjectViewsSerializerFG
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" in params:
            start_date = datetime.datetime.strptime(params.pop("posted_date__gte"), "%Y-%m-%d")
            end_date = datetime.datetime.strptime(params.pop("posted_date__lte"), "%Y-%m-%d")
        else:
            end_date = datetime.datetime.strptime(settings.DEFAULT_TO_DATE, "%Y-%m-%d")
            start_date = datetime.datetime.strptime(settings.DEFAULT_FROM_DATE, "%Y-%m-%d")

        self.queryset = []

        filters = {
            "title__title__in": params["key_word__in"],
            "article__object__in": params["object__in"]
        }

        aggregator = "article__object"

        for info in UploadedInfo.objects.filter(provider__title="factrum_group"):
            if not info.is_in_period(start_date, end_date, "week"):
                continue
            self.queryset += info.factrum_group.filter(**filters).values(aggregator).annotate(
                views=Sum("views")).values(
                "views", aggregator, "upload_info__title")

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        serializer = self.get_serializer(data=list(queryset), many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class ObjectAdmixerViews(KeywordAdmixerViews):
    serializer_class = ObjectViewsSerializerAdmixer

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" not in params:
            end_date = settings.DEFAULT_TO_DATE
            start_date = settings.DEFAULT_FROM_DATE
        else:
            end_date = params["posted_date__lte"]
            start_date = params["posted_date__gte"]

        self.queryset = []

        publications = Publication.objects.filter(**params).values_list("object", "shukachpublication__shukach_id")
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

        for obj, ids in l_part.items():
            results = {}

            for batch_ids in self._chunks(ids, 10000):
                logger.info("Sent %d ids" % len(batch_ids))
                results = self._query_admixer_data(results, batch_ids, start_date, end_date)
                current += len(batch_ids)
                logger.info("Processed: %d/%d" % (current, total))

            for date, views in results.items():
                self.queryset.append({
                    "object": obj,
                    "views": views,
                    "date": date,
                })

        self._client.disconnect()

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        return Response(queryset)


class KeywordFactrumSdViews(generics.ListAPIView):
    serializer_class = ThemeCompanySdViewsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" in params:
            start_date = datetime.datetime.strptime(params.pop("posted_date__gte"), "%Y-%m-%d")
            end_date = datetime.datetime.strptime(params.pop("posted_date__lte"), "%Y-%m-%d")
        else:
            end_date = datetime.datetime.strptime(settings.DEFAULT_TO_DATE, "%Y-%m-%d")
            start_date = datetime.datetime.strptime(settings.DEFAULT_FROM_DATE, "%Y-%m-%d")

        self.queryset = []

        display_fields = ["views", "title__title", "upload_info__title", 'sex', 'age', 'education', 'children_lt_16',
                          'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP']

        for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
            if not info.is_in_period(start_date, end_date, "week"):
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


class ObjectFactrumSdViews(generics.ListAPIView):
    serializer_class = ObjectSdViewsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    fg_values_list = ('views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def _concat_dict(self, ld, rd):
        return dict([(k, ld[k] + rd[k] if k in rd else 0) for k in ld.keys()])

    def _transform_concat(self, total, sd, all, specific, group):
        views = sd["views"] / all * specific
        calculate = lambda v: v / 100.0 * views
        converter = lambda d: dict([(k, int(calculate(v))) for k, v in d.items()])
        return {
            "views": int(total["views"] + views),
            "sex": self._concat_dict(total["sex"], converter(sd["sex"])),
            "age": self._concat_dict(total["age"], converter(sd["age"])),
            "education": self._concat_dict(total["education"], converter(sd["education"])),
            "children_lt_16": self._concat_dict(total["children_lt_16"], converter(sd["children_lt_16"])),
            "marital_status": self._concat_dict(total["marital_status"], converter(sd["marital_status"])),
            "occupation": self._concat_dict(total["occupation"], converter(sd["occupation"])),
            "group": self._concat_dict(total["group"], converter(sd["group"])),
            "income": self._concat_dict(total["income"], converter(sd["income"])),
            "region": self._concat_dict(total["region"], converter(sd["region"])),
            "typeNP": self._concat_dict(total["typeNP"], converter(sd["typeNP"]))
        }

    def _build_object_sd(self, info, params):

        instance = info.factrum_group_detailed.filter(title__title__in=params["key_word__in"]).first()

        instance_sd = dict(zip(
            self.fg_values_list,
            [getattr(instance, field) for field in self.fg_values_list]))

        start_date = datetime.datetime.strptime(instance.upload_info.title, "%w-%W-%Y")
        end_date = start_date + datetime.timedelta(days=6)

        query_params = {
            'posted_date__gte': start_date,
            'posted_date__lte': end_date,
            'key_word__in': params["key_word__in"]
        }

        theme_rating = Publication.objects.filter(**query_params).values_list(
            "key_word").annotate(publication_amount=Count("key_word")).order_by("-publication_amount").first()[1]

        query_params["object__in"] = params["object__in"]

        object_rating = Publication.objects.filter(**query_params).values_list(
            "object").annotate(publication_amount=Count("key_word")).order_by("-publication_amount").first()[1]

        init_sd = {}
        info = SocialDetailsSerializer()
        for key in self.fg_values_list[1:]:
            keys = [str(f) for f in info.fields[key].keys]
            init_sd[key] = dict(zip(keys, [0] * len(keys)))
        init_sd["views"] = 0

        sd = self._transform_concat(init_sd, instance_sd, theme_rating, object_rating, params["sd"])
        sd["object"] = params["object__in"][0]
        sd["date"] = end_date.strftime("%Y-%m-%d")

        return [sd]

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if "posted_date__lte" in params:
            start_date = datetime.datetime.strptime(params.pop("posted_date__gte"), "%Y-%m-%d")
            end_date = datetime.datetime.strptime(params.pop("posted_date__lte"), "%Y-%m-%d")
        else:
            end_date = datetime.datetime.strptime(settings.DEFAULT_TO_DATE, "%Y-%m-%d")
            start_date = datetime.datetime.strptime(settings.DEFAULT_FROM_DATE, "%Y-%m-%d")

        self.queryset = []

        for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
            if not info.is_in_period(start_date, end_date, "week"):
                continue
            self.queryset += self._build_object_sd(info, params)

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        return Response(queryset)


class KeywordAdmixerSdViews(generics.ListAPIView):
    serializer_class = DynamicAnalyzedInfoSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    admixer_values_list = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views', 'date')

    def _chunks(self, l, n):
        return [l[i:i + n] for i in range(0, len(l), n)]

    def _convert(self, tup):
        di = {}
        for a, b in tup:
            di.setdefault(a, []).append(b)
        return di

    def _init(self, request):
        params = handle_request_params(request)
        params.pop('sd')

        if "posted_date__lte" not in params:
            self.end_date = settings.DEFAULT_TO_DATE
            self.start_date = settings.DEFAULT_FROM_DATE
        else:
            self.end_date = params["posted_date__lte"]
            self.start_date = params["posted_date__gte"]

        publications = Publication.objects.filter(**params).values_list("key_word", "shukachpublication__shukach_id")
        logger.info("Get %d publications" % len(publications))

        self.l_part = self._convert(publications)
        logger.info("Convert %d publications" % len(publications))

        self._client = Client(settings.CLICKHOUSE_HOST,
                              database=settings.CLICKHOUSE_DB,
                              user=settings.CLICKHOUSE_USER,
                              password=settings.CLICKHOUSE_PASSWORD)

        self.total = len(publications)
        self.current = 0
        self.queryset = []

    def get(self, request, *args, **kwargs):

        self._init(request)

        for key_word, ids in self.l_part.items():
            results = {}
            for batch_ids in self._chunks(ids, 10000):
                logger.info("Sent %d ids" % len(batch_ids))
                results = self._query_admixer_data(results, batch_ids, self.start_date, self.end_date)
                self.current += len(batch_ids)
                logger.info("Processed: %d/%d" % (self.current, self.total))
            for date, items in results.items():
                row = {
                    "key_word": key_word,
                    "views": items["views"],
                    "uniques": items["uniques"],
                    "date": date
                }
                for key in self.admixer_values_list[:-3]:
                    row[key] = dict(Counter(items[key]))
                self.queryset.append(row)

        self._client.disconnect()

        return self.list(request, *args, **kwargs)

    def _query_admixer_data(self, results, batch_ids, start_date, end_date):
        ids = ",".join("'%s'" % item for item in batch_ids)

        query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views), Date ' \
                'from admixer.UrlStat ' \
                'where UrlId in (%s) and Date >= \'%s\' and Date <= \'%s\' ' \
                'Group by UrlId, Platform, Browser, Country, Age, Gender, Income, Date' % (ids, start_date, end_date)

        response = self._client.execute(query)
        keys = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views', 'date')
        for row in response:
            item = dict(zip(keys, row[1:]))
            date = item["date"] - datetime.timedelta(days=item["date"].weekday())
            if date not in results:
                results[date] = {}
                results[date]['uniques'] = item['uniques']
                results[date]['views'] = item['views']
                for key in keys[:-3]:
                    results[date][key] = [item[key]]
            else:
                for key in keys[:-3]:
                    results[date][key].append(item[key])
                results[date]['uniques'] += item['uniques']
                results[date]['views'] += item['views']

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        return Response(queryset)


class ObjectAdmixerSdViews(KeywordAdmixerSdViews):
    serializer_class = DynamicAnalyzedInfoSerializer

    def get(self, request, *args, **kwargs):

        self._init(request)

        for obj, ids in self.l_part.items():
            results = {}
            for batch_ids in self._chunks(ids, 10000):
                logger.info("Sent %d ids" % len(batch_ids))
                results = self._query_admixer_data(results, batch_ids, self.start_date, self.end_date)
                self.current += len(batch_ids)
                logger.info("Processed: %d/%d" % (self.current, self.total))
            for date, items in results.items():
                row = {
                    "object": obj,
                    "views": items["views"],
                    "uniques": items["uniques"],
                    "date": date
                }
                for key in self.admixer_values_list[:-3]:
                    row[key] = dict(Counter(items[key]))
                self.queryset.append(row)

        self._client.disconnect()

        return self.list(request, *args, **kwargs)


class ThemeList(generics.ListAPIView):
    queryset = Publication.objects.values("market__name").annotate(keywords=ArrayAgg("key_word"))
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def list(self, request, *args, **kwargs):
        return Response(
            [{"market": item["market__name"], "keywords": set(item["keywords"])} for item in self.get_queryset()])


class ObjectsList(generics.ListAPIView):
    queryset = Publication.objects
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if len(params):
            self.queryset = Publication.objects.filter(**params).values("key_word").annotate(objects=ArrayAgg("object"))
        else:
            self.queryset = Publication.objects.values("key_word").annotate(objects=ArrayAgg("object"))

        return self.list(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        return Response(
            [{"key_word": item["key_word"], "objects": set(item["objects"])} for item in self.get_queryset()])