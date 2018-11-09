# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
from collections import Counter
from itertools import groupby

from sortedcontainers import SortedListWithKey
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Sum, Count, Q
from django.views.generic import TemplateView
from django.conf import settings
from rest_framework import generics
from rest_framework import permissions
from rest_framework_csv import renderers as r
from rest_framework.response import Response

from clickhouse_driver import Client
from aggregator.pagination import ConfiguredPageNumberPagination
from aggregator.permissions import *
from aggregator.serializers import *
from factrum_group.serializers import PublicationsSocialDemoRatingSerializer
from factrum_group.models import AnalyzedInfo as FG, SocialDetails, PublicationsSocialDemoRating
from admixer.models import AnalyzedInfo as Admixer
from noksfishes.models import ShukachPublication, Theme, Publication, Market
from datetime import datetime, timedelta

from uploaders.models import UploadedInfo

import logging
logger = logging.getLogger(__name__)

r.CSVRenderer.writer_opts = {
    "delimiter": str(u';')
}


def handle_request_params(request):
    params = dict(request.query_params.items())
    params.pop("page")
    params.pop("per_page")

    if not request.user.has_perm('global_permissions.free_time'):
        params["posted_date__lte"] = "2018-12-30"
        params["posted_date__gte"] = "2018-07-01"
        # end_date = datetime.now().replace(day=1) - timedelta(days=1)
        # params["posted_date__lte"] = end_date.strftime("%Y-%m-%d")
        # params["posted_date__gte"] = end_date.replace(day=1).strftime("%Y-%m-%d")
    if "market__in" in params:
        params["market__in"] = json.loads(params.pop("market__in"))
        if not len(params["market__in"]):
            params.pop("market__in")
    if "key_word__in" in params:
        params["key_word__in"] = json.loads(params.pop("key_word__in"))
        if not len(params["key_word__in"]):
            params.pop("key_word__in")
    return params


class GeneralInfoRenderer(r.CSVRenderer):
    header = ['shukach_id', 'url', 'factrum_views', 'admixer_views']


class SocialDetailsRenderer(r.CSVRenderer):
    header = ['id_theme', 'theme', 'factrum_views', 'admixer_views', 'uniques_admixer', 'sex_factrum', 'sex_admixer',
              'age_factrum', 'age_admixer', 'region_factrum', 'region_admixer', 'income_factrum', 'income_admixer',
              'education', 'children_lt_16', 'marital_status', 'occupation', 'group', 'typeNP', 'platform', 'browser']


class HomeView(LoginRequiredMixin, TemplateView):
    login_url = '/login/'
    template_name = "index.html"


class FactrumAdmixerGeneralInfoList(generics.ListAPIView):
    renderer_classes = (GeneralInfoRenderer,)
    serializer_class = FactrumAdmixerGeneralSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def list(self, request, *args, **kwargs):
        queryset = []
        fg_map = {}
        for article_id, url, views in FG.objects.all().values_list("article__id", "article__url", "views").distinct():
            fg_map[article_id] = (url, views)

        for url_id, admixer_views in Admixer.objects.values_list("url_id").annotate(views=Sum("views")):
            pid = ShukachPublication.objects.select_related("publication").filter(
                shukach_id=url_id).first().publication.id
            if pid in fg_map:
                url, fg_views = fg_map[pid]
                queryset.append({
                    'shukach_id': url_id,
                    'url': url,
                    'factrum_views': fg_views,
                    'admixer_views': admixer_views
                })

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)


class FactrumAdmixerSocialDetailsList(generics.ListAPIView):
    renderer_classes = (SocialDetailsRenderer,)
    serializer_class = FactrumAdmixerSocialDetailsSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    fg_values_list = ('title__id', 'title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    fg_mapped_keys = ('theme', 'factrum_views', 'sex_factrum', 'age_factrum', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income_factrum', 'region_factrum', 'typeNP')

    admixer_values_list = ('url_id', "views", "platform", "browser", "region", "age", "gender", "income", 'uniques')

    admixer_mapped_keys = ('admixer_views', 'platform', 'browser', 'region_admixer', 'age_admixer', 'sex_admixer',
                           'income_admixer', 'uniques_admixer')

    def list(self, request, *args, **kwargs):
        queryset = []
        fg_map = {}

        for row in SocialDetails.objects.all().values_list(*self.fg_values_list):
            fg_map[row[0]] = dict(zip(self.fg_mapped_keys, row[1:]))

        admixer_collection = {}
        title_keywords = {}
        themes = dict(Theme.objects.values_list("title", "id"))

        admixer_aggregators = ('platform', 'browser', 'region_admixer', 'age_admixer', 'sex_admixer', 'income_admixer')

        for row in Admixer.objects.values_list(
                "url_id").annotate(views=Sum("views"),
                                   platform=ArrayAgg("platform"),
                                   browser=ArrayAgg("browser"),
                                   region=ArrayAgg("region"),
                                   age=ArrayAgg("age"),
                                   gender=ArrayAgg("gender"),
                                   income=ArrayAgg("income"),
                                   uniques=Sum('uniques')).values_list(*self.admixer_values_list):
            for title in ShukachPublication.objects.select_related("publication").filter(shukach_id=row[0]).values_list(
                    "publication__key_word", flat=True).distinct():
                id_title = themes[title]
                if id_title in admixer_collection:
                    if row[0] not in title_keywords[id_title]:
                        title_keywords[id_title][row[0]] = None
                        for index, key in enumerate(self.admixer_mapped_keys):
                            admixer_collection[id_title][key] += row[index + 1]
                else:
                    admixer_collection[id_title] = dict(zip(self.admixer_mapped_keys, row[1:]))
                    title_keywords[id_title] = dict([(row[0], None)])

        for id_title, sd in admixer_collection.items():
            if id_title in fg_map:
                for aggregated_field in admixer_aggregators:
                    sd[aggregated_field] = dict(Counter(sd[aggregated_field]))

                item = dict(fg_map[id_title])
                item.update(sd)
                item["id_theme"] = id_title
                queryset.append(item)

        serializer = self.get_serializer(data=queryset, many=True)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class MarketsRating(generics.ListAPIView):
    queryset = Market.objects
    serializer_class = MarketsRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if len(params):
            params["market_publications__posted_date__lte"] = params.pop("posted_date__lte")
            params["market_publications__posted_date__gte"] = params.pop("posted_date__gte")
            self.queryset = Market.objects.filter(**params).values(
                "id", "name").annotate(publication_amount=Count('market_publications')).order_by("-publication_amount")
        else:
            self.queryset = Market.objects.values(
                "id", "name").annotate(publication_amount=Count("market_publications")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class ThemeCompanyRating(generics.ListAPIView):
    queryset = Publication.objects
    serializer_class = ThemeCompanyRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "key_word").annotate(publication_amount=Count("key_word")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "key_word").annotate(publication_amount=Count("key_word")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class PublicationRating(generics.ListAPIView):
    queryset = Publication.objects
    serializer_class = PublicationRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToPublicationAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "publication", "country", "region", "city", "type", "topic", "consolidated_type").annotate(
                publication_amount=Count("publication")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "publication", "country", "region", "city", "type", "topic", "consolidated_type").annotate(
                publication_amount=Count("publication")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class SpecificSocialDemoRatingAdmixer(generics.ListAPIView):
    serializer_class = SocialDemoRatingAdmixerSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow)
    pagination_class = ConfiguredPageNumberPagination
    admixer_values_list = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views')

    def _chunks(self, l, n):
        return [l[i:i+n] for i in range(0, len(l), n)]

    def _convert(self, tup):
        di = {}
        for a, b in tup:
            di.setdefault(a, []).append(b)
        return di

    def _query_admixer_data(self, batch_ids, start_date, end_date):
        ids = ",".join("'%s'" % item for item in batch_ids)

        query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views)' \
                'from admixer.UrlStat ' \
                'where UrlId in (%s) and Date >= \'%s\' and Date <= \'%s\' ' \
                'Group by UrlId, Platform, Browser, Country, Age, Gender, Income' % (ids, start_date, end_date)

        response = self._client.execute(query)
        results = dict(zip(self.admixer_values_list, [[], [], [], [], [], [], 0, 0]))
        for row in response:
            item = dict(zip(self.admixer_values_list, row[1:]))
            for key in self.admixer_values_list[:-2]:
                results[key].append(item[key])
            for key in self.admixer_values_list[len(self.admixer_values_list)-2:]:
                results[key] += (item[key])

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def list(self, request, *args, **kwargs):

        params = handle_request_params(request)

        # TODO validate using form/serializer query params
        if not len(params):
            return Response()

        try:
            end_date = params.pop("posted_date__lte")
            start_date = params.pop("posted_date__gte")
        except KeyError as e:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

        publications = Publication.objects.filter(**params).values_list("key_word", "shukachpublication__shukach_id")
        l_part = self._convert(publications)

        self._client = Client(settings.CLICKHOUSE_HOST,
                              database=settings.CLICKHOUSE_DB,
                              user=settings.CLICKHOUSE_USER,
                              password=settings.CLICKHOUSE_PASSWORD)

        total = len(publications)
        current = 0
        queryset = []

        for key_word, ids in l_part.items():
            results = dict(zip(self.admixer_values_list, [[], [], [], [], [], [], 0, 0]))
            for batch_ids in self._chunks(ids, 10000):
                logger.info("Sent %d ids" % len(batch_ids))
                item = self._query_admixer_data(batch_ids, start_date, end_date)
                for key in self.admixer_values_list:
                    results[key] += item[key]
                current += len(batch_ids)
                logger.info("Processed: %d/%d" % (current, total))
            queryset.append({
                "aggregator": key_word,
                "views": results["views"],
                "platforms": dict(Counter(results["platform"])),
                "browsers": dict(Counter(results["browser"])),
                "regions": dict(Counter(results["region"])),
                "age_groups": dict(Counter(results["age"])),
                "gender_groups": dict(Counter(results["gender"])),
                "income_groups": dict(Counter(results["income"])),
                "uniques": results["uniques"]
            })

        self._client.disconnect()

        page = self.paginate_queryset(queryset)
        if page is not None:
            return self.get_paginated_response(queryset)
        return Response(queryset)


class GeneralSocialDemoRatingAdmixer(generics.ListAPIView):
    serializer_class = SocialDemoRatingAdmixerSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow)
    pagination_class = ConfiguredPageNumberPagination
    admixer_values_list = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views')

    def _chunks(self, l, n):
        return [l[i:i+n] for i in range(0, len(l), n)]

    def _convert(self, tup):
        di = {}
        for a, b in tup:
            di.setdefault(a, []).append(b)
        return di

    def _query_admixer_data(self, batch_ids, start_date, end_date):
        ids = ",".join("'%s'" % item for item in batch_ids)

        query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views)' \
                'from admixer.UrlStat ' \
                'where UrlId in (%s) and Date >= \'%s\' and Date <= \'%s\' ' \
                'Group by UrlId, Platform, Browser, Country, Age, Gender, Income' % (ids, start_date, end_date)

        response = self._client.execute(query)
        results = dict(zip(self.admixer_values_list, [[], [], [], [], [], [], 0, 0]))
        for row in response:
            item = dict(zip(self.admixer_values_list, row[1:]))
            for key in self.admixer_values_list[:-2]:
                results[key].append(item[key])
            for key in self.admixer_values_list[len(self.admixer_values_list)-2:]:
                results[key] += (item[key])

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def list(self, request, *args, **kwargs):

        params = handle_request_params(request)

        aggregator = params.pop("aggregator", None)

        if not aggregator:
            return Response()

        try:
            end_date = params.pop("posted_date__lte")
            start_date = params.pop("posted_date__gte")
        except KeyError as e:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

        publications = Publication.objects.filter(**params).values_list(aggregator, "shukachpublication__shukach_id")
        l_part = self._convert(publications)

        self._client = Client(settings.CLICKHOUSE_HOST,
                              database=settings.CLICKHOUSE_DB,
                              user=settings.CLICKHOUSE_USER,
                              password=settings.CLICKHOUSE_PASSWORD)

        total = len(publications)
        current = 0
        queryset = []

        for agg, ids in l_part.items():
            results = dict(zip(self.admixer_values_list, [[], [], [], [], [], [], 0, 0]))
            for batch_ids in self._chunks(ids, 10000):
                logger.info("Sent %d ids" % len(batch_ids))
                item = self._query_admixer_data(batch_ids, start_date, end_date)
                for key in self.admixer_values_list:
                    results[key] += item[key]
                current += len(batch_ids)
                logger.info("Processed: %d/%d" % (current, total))
            queryset.append({
                "aggregator": agg,
                "views": results["views"],
                "platforms": dict(Counter(results["platform"])),
                "browsers": dict(Counter(results["browser"])),
                "regions": dict(Counter(results["region"])),
                "age_groups": dict(Counter(results["age"])),
                "gender_groups": dict(Counter(results["gender"])),
                "income_groups": dict(Counter(results["income"])),
                "uniques": results["uniques"]
            })

        self._client.disconnect()

        page = self.paginate_queryset(queryset)
        if page is not None:
            return self.get_paginated_response(queryset)
        return Response(queryset)


class SpecialByThemeSocialDemoRatingFG(generics.ListAPIView):
    serializer_class = SocialDemoRatingFGSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = handle_request_params(request)

        # TODO validate using form/serializer query params
        if not len(params):
            return Response()

        queryset = []
        start_date = datetime.strptime(params['posted_date__gte'], "%Y-%m-%d")
        end_date = datetime.strptime(params['posted_date__lte'], "%Y-%m-%d")
        for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
            if not info.is_in_period(start_date, end_date):
                # TODO make flag for week or month
                continue
            queryset += info.factrum_group_detailed.filter(
                title__title__in=params['key_word__in']).values(*self.fg_values_list).order_by("-views")

        serializer = self.get_serializer(data=queryset, many=True)
        serializer.is_valid(raise_exception=True)

        result = []
        for key, group in groupby(serializer.data, lambda x: x['title__title']):
            item = dict(zip(self.fg_values_list, [{} for i in range(len(self.fg_values_list))]))
            item['title__title'] = key
            item['views'] = 0
            for row in group:
                item['views'] += row['views']
                for sd, sub_sd in row.items():
                    if type(sub_sd) is dict:
                        for k, v in sub_sd.items():
                            try:
                                item[sd][k] += v
                            except KeyError:
                                item[sd][k] = v
            result.append(item)

        page = self.paginate_queryset(result)
        if page is not None:
            return self.get_paginated_response(page)

        return Response(result)


class SpecialByThemePublicationSocialDemoRatingFG(generics.ListAPIView):
    serializer_class = SocialDemoRatingFGSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow,
                          IsRequestsToPublicationAllow, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = handle_request_params(request)

        # TODO validate using form/serializer query params
        if not len(params):
            return Response()

        sd = []
        start_date = datetime.strptime(params['posted_date__gte'], "%Y-%m-%d")
        end_date = datetime.strptime(params['posted_date__lte'], "%Y-%m-%d")
        for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
            if not info.is_in_period(start_date, end_date):
                # TODO make flag for week or month
                continue
            sd += info.factrum_group_detailed.filter(
                title__title__in=params['key_word__in']).values(*self.fg_values_list)

        if not len(sd):
            return Response([])

        records_in_theme_by_publication = Publication.objects.filter(**params).count()

        params.pop('publication')

        all_records_in_theme = Publication.objects.filter(**params).count()
        self.format_kwarg = {'all': float(all_records_in_theme), "specific": float(records_in_theme_by_publication)}

        page = self.paginate_queryset(sd)
        if page is not None:
            serializer = self.get_serializer(data=page[0])
            serializer.is_valid(raise_exception=True)
            return self.get_paginated_response(serializer.data)

        serializer = self.get_serializer(data=sd[0])
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data)


class GeneralByThemesSocialDemoRatingFG(generics.ListAPIView):
    serializer_class = SocialDemoRatingFGSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = handle_request_params(request)
        params.pop("aggregator")
        queryset = []
        if len(params):
            start_date = datetime.strptime(params['posted_date__gte'], "%Y-%m-%d")
            end_date = datetime.strptime(params['posted_date__lte'], "%Y-%m-%d")
            for info in UploadedInfo.objects.filter(provider__title="factrum_group_social"):
                if not info.is_in_period(start_date, end_date):
                    # TODO make flag for week or month
                    continue
                queryset += info.factrum_group_detailed.values(*self.fg_values_list).order_by("-views")
        else:
            queryset = SocialDetails.objects.all().values(*self.fg_values_list).order_by("-views")

        serializer = self.get_serializer(data=queryset, many=True)
        serializer.is_valid(raise_exception=True)

        result = []
        for key, group in groupby(serializer.data, lambda x: x['title__title']):
            item = dict(zip(self.fg_values_list, [{} for i in range(len(self.fg_values_list))]))
            item['title__title'] = key
            item['views'] = 0
            for row in group:
                item['views'] += row['views']
                for sd, sub_sd in row.items():
                    if type(sub_sd) is dict:
                        for k, v in sub_sd.items():
                            try:
                                item[sd][k] += v
                            except KeyError:
                                item[sd][k] = v
            result.append(item)

        if len(result):
            page = self.paginate_queryset(result)
            if page is not None:
                return self.get_paginated_response(page)

        return Response(result)


class GeneralByPublicationsSocialDemoRatingFG(generics.ListAPIView):
    serializer_class = PublicationsSocialDemoRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow, IsRequestsToPublicationAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = handle_request_params(request)
        params.pop("aggregator")

        if len(params):
            self.queryset = PublicationsSocialDemoRating.objects.filter(
                created_date__gte=params['posted_date__gte'],
                created_date__lte=params['posted_date__lte']).order_by("-views")
            if "publication" in params:
                self.queryset = self.queryset.filter(publication=request.query_params['publication']).order_by("-views")
        else:
            self.queryset = PublicationsSocialDemoRating.objects.all().order_by("-views")

        return self.list(request, *args, **kwargs)
