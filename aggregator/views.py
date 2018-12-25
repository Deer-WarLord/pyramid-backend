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


class ParamsHandler:
    def handle_request_params(self, request):
        params = dict(request.query_params.items())
        params.pop("page")
        params.pop("per_page")

        if "format" in params:
            self.format = params.pop("format")

        if not request.user.has_perm('global_permissions.free_time'):
            params["posted_date__lte"] = settings.DEFAULT_TO_DATE
            params["posted_date__gte"] = settings.DEFAULT_FROM_DATE
        if "market__in" in params:
            params["market__in"] = json.loads(params.pop("market__in"))
            if not len(params["market__in"]):
                params.pop("market__in")
        if "key_word__in" in params:
            params["key_word__in"] = json.loads(params.pop("key_word__in"))
            if not len(params["key_word__in"]):
                params.pop("key_word__in")
        if "publication__in" in params:
            params["publication__in"] = json.loads(params.pop("publication__in"))
            if not len(params["publication__in"]):
                params.pop("publication__in")
        if "region__in" in params:
            params["region__in"] = json.loads(params.pop("region__in"))
            if not len(params["region__in"]):
                params.pop("region__in")
        if "type__in" in params:
            params["type__in"] = json.loads(params.pop("type__in"))
            if not len(params["type__in"]):
                params.pop("type__in")
        if "topic__in" in params:
            params["topic__in"] = json.loads(params.pop("topic__in"))
            if not len(params["topic__in"]):
                params.pop("topic__in")

        return params


class GeneralInfoRenderer(r.CSVRenderer):
    header = ['shukach_id', 'url', 'factrum_views', 'admixer_views']


class SocialDetailsRenderer(r.CSVRenderer):
    header = ['id_theme', 'theme', 'factrum_views', 'admixer_views', 'uniques_admixer', 'sex_factrum', 'sex_admixer',
              'age_factrum', 'age_admixer', 'region_factrum', 'region_admixer', 'income_factrum', 'income_admixer',
              'education', 'children_lt_16', 'marital_status', 'occupation', 'group', 'typeNP', 'platform', 'browser']


class FGRenderer(r.PaginatedCSVRenderer):
    labels = {
        "title__title": "Ключи",
        "publication": "СМИ",
        "views": "Просмотры",
        "sex.male": "Мужчин",
        "sex.female": "Женщин",
        "age.15-17": "Возраст от 15 до 17",
        "age.18-24": "Возраст от 18 до 24",
        "age.25-34": "Возраст от 25 до 34",
        "age.35-44": "Возраст от 35 до 44",
        "age.45+": "Возраст после 45",
        "education.lte9": "С не полным среднем",
        "education.11": "С среднем",
        "education.bachelor": "С не полным высшим",
        "education.master": "С высшим",
        "children_lt_16.yes": "Дети младше 16 Есть",
        "children_lt_16.no": "Дети младше 16 Нету",
        "marital_status.single": "Не женатых/замужем",
        "marital_status.married": "Женатых/Замужем",
        "marital_status.widow(er)": "Вдовцов/Вдов",
        "marital_status.divorced": "Разведенных",
        "marital_status.liveTogether": "Проживающих вместе",
        "occupation.businessOwner": "Владелецев бизнеса с наёмными сотрудниками",
        "occupation.entrepreneur": "Частных предпринимателей",
        "occupation.hiredManager": "Наёмных руководителей",
        "occupation.middleManager": "Руководителей среднего звена",
        "occupation.masterDegreeSpecialist": "Специалистов с высшим образованием",
        "occupation.employee": "Служащих",
        "occupation.skilledWorker": "Квалифицированных рабочих",
        "occupation.otherWorkers": "Других рабочих и технического персонала",
        "occupation.mobileWorker": "Мобильных работников",
        "occupation.militaryPoliceman": "Военнослужащих/Сотрудников правоохранительных органов",
        "occupation.student": "Студентов/Школьников",
        "occupation.pensioner": "Пенсионеров",
        "occupation.disabled": "Инвалидов",
        "occupation.housewife": "Домохозяек",
        "occupation.maternityLeave": "В декретном отпуске",
        "occupation.temporarilyUnemployed": "Временно безработных/ищущих работу",
        "occupation.other": "Другие",
        "group.1": "Не имеют достаточно денег для приобретения продуктов",
        "group.2": "Имеют достаточно денег для приобретения продуктов",
        "group.3": "Имеют достаточно денег для приобретения продуктов и одежды",
        "group.4": "Имеют достаточно денег для приобретения товаров длительного пользования",
        "group.5": "Могут позволить себе покупать действительно дорогие вещи",
        "income.noAnswer": "Доход Не ответили",
        "income.0-1000": "Доход до 1000",
        "income.1001-2000": "Доход от 1000 до 2000",
        "income.2001-3000": "Доход от 2000 до 3000",
        "income.3001-4000": "Доход от 3000 до 4000",
        "income.4001-5000": "Доход от 4000 до 5000",
        "income.gt5001": "Доход более 5000",
        "region.west": "Запад",
        "region.center": "Центр",
        "region.east": "Восток",
        "region.south": "Юг",
        "typeNP.50+": "Тип НП 50+",
        "typeNP.50-": "Тип НП 50-"
    }


class AdmixerRenderer(r.PaginatedCSVRenderer):
    labels = {
        "uniques": "Уникальных пользователей",
        "views": "Просмотров",
        "0": "Пол Неизвестно",
        "1": "Мужчин",
        "2": "Женщин",
        "age_groups.0": "Возраст Неизвестно",
        "age_groups.1": "Возраст до 18",
        "age_groups.2": "Возраст от 18 до 24",
        "age_groups.3": "Возраст от 25 до 34",
        "age_groups.4": "Возраст от 35 до 44",
        "age_groups.5": "Возраст после 45",
        "browsers.0": "Браузер Неизвестно",
        "browsers.1": "IE",
        "browsers.2": "Firefox",
        "browsers.3": "Chrome",
        "browsers.4": "Safari",
        "browsers.5": "Opera",
        "browsers.6": "Yandex",
        "browsers.7": "IE7andLower",
        "browsers.8": "IE8",
        "browsers.9": "IE9",
        "browsers.10": "IE10",
        "browsers.11": "IE11",
        "browsers.12": "Edge",
        "platforms.0": "Неизвестно",
        "platforms.1": "IPad",
        "platforms.2": "IPod",
        "platforms.3": "IPhone",
        "platforms.4": "Windows_Phone_7",
        "platforms.5": "Android_tablet",
        "platforms.6": "Android_phone",
        "platforms.7": "BlackBerry",
        "platforms.8": "Symbian",
        "platforms.9": "Bada",
        "platforms.10": "Win8_tablet",
        "platforms.11": "Win_phone_8",
        "platforms.12": "Palm",
        "platforms.13": "Motorola",
        "platforms.14": "WinCE",
        "platforms.15": "Win95",
        "platforms.16": "Win98",
        "platforms.17": "WinME",
        "platforms.18": "Win2000",
        "platforms.19": "WinXP",
        "platforms.20": "WinVista",
        "platforms.21": "Win7",
        "platforms.22": "Win8",
        "platforms.23": "WinRT",
        "platforms.24": "Mac",
        "platforms.25": "Linux",
        "platforms.26": "Irix",
        "platforms.27": "Sun",
        "platforms.28": "Win10",
        "platforms.29": "Win_phone_10"
    }


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


class MarketsRating(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, r.PaginatedCSVRenderer)
    queryset = Market.objects
    serializer_class = MarketsRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        if len(params):
            params["market_publications__posted_date__lte"] = params.pop("posted_date__lte")
            params["market_publications__posted_date__gte"] = params.pop("posted_date__gte")
            self.queryset = Market.objects.filter(**params).values(
                "id", "name").annotate(publication_amount=Count('market_publications')).order_by("-publication_amount")
        else:
            self.queryset = Market.objects.values(
                "id", "name").annotate(publication_amount=Count("market_publications")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class ThemeCompanyRating(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, r.PaginatedCSVRenderer)
    queryset = Publication.objects
    serializer_class = ThemeCompanyRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "key_word").annotate(publication_amount=Count("key_word")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "key_word").annotate(publication_amount=Count("key_word")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class RegionRating(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, r.PaginatedCSVRenderer)
    queryset = Publication.objects
    serializer_class = RegionRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        if "region__in" in params:
            params.pop("region__in")

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "region").annotate(publication_amount=Count("region")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "region").annotate(publication_amount=Count("region")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class PublicationTypeRating(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, r.PaginatedCSVRenderer)
    queryset = Publication.objects
    serializer_class = PublicationTypeRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        if "type__in" in params:
            params.pop("type__in")

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "type").annotate(publication_amount=Count("type")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "type").annotate(publication_amount=Count("type")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class PublicationTopicRating(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, r.PaginatedCSVRenderer)
    queryset = Publication.objects
    serializer_class = PublicationTopicRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        if "topic__in" in params:
            params.pop("topic__in")

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "topic").annotate(publication_amount=Count("topic")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "topic").annotate(publication_amount=Count("topic")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class PublicationRating(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, r.PaginatedCSVRenderer)
    queryset = Publication.objects
    serializer_class = PublicationRatingSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToPublicationAllow)
    pagination_class = ConfiguredPageNumberPagination

    def get(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        if len(params):
            self.queryset = Publication.objects.filter(**params).values(
                "publication", "country", "region", "city", "type", "topic", "consolidated_type").annotate(
                publication_amount=Count("publication")).order_by("-publication_amount")
        else:
            self.queryset = Publication.objects.values(
                "publication", "country", "region", "city", "type", "topic", "consolidated_type").annotate(
                publication_amount=Count("publication")).order_by("-publication_amount")

        return self.list(request, *args, **kwargs)


class SpecificSocialDemoRatingAdmixer(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, AdmixerRenderer)
    serializer_class = SocialDemoRatingAdmixerSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow)
    pagination_class = ConfiguredPageNumberPagination
    admixer_values_list = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views')

    def _chunks(self, l, n):
        return [l[i:i + n] for i in range(0, len(l), n)]

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
            for key in self.admixer_values_list[len(self.admixer_values_list) - 2:]:
                results[key] += (item[key])

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def list(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        # TODO validate using form/serializer query params
        if not len(params):
            return Response()

        try:
            end_date = params.pop("posted_date__lte")
            start_date = params.pop("posted_date__gte")
        except KeyError as e:
            end_date = datetime.strptime(settings.DEFAULT_TO_DATE, "%Y-%m-%d")
            start_date = datetime.strptime(settings.DEFAULT_FROM_DATE, "%Y-%m-%d")

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


class GeneralSocialDemoRatingAdmixer(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, AdmixerRenderer)
    serializer_class = SocialDemoRatingAdmixerSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow)
    pagination_class = ConfiguredPageNumberPagination
    admixer_values_list = ('platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views')

    def _chunks(self, l, n):
        return [l[i:i + n] for i in range(0, len(l), n)]

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
            for key in self.admixer_values_list[len(self.admixer_values_list) - 2:]:
                results[key] += (item[key])

        logger.info("Received %d records from ClickHouse", len(results))

        return results

    def list(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

        aggregator = params.pop("aggregator", None)

        if not aggregator:
            return Response()

        try:
            end_date = params.pop("posted_date__lte")
            start_date = params.pop("posted_date__gte")
        except KeyError as e:
            end_date = datetime.strptime(settings.DEFAULT_TO_DATE, "%Y-%m-%d")
            start_date = datetime.strptime(settings.DEFAULT_FROM_DATE, "%Y-%m-%d")

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


class SpecialByThemeSocialDemoRatingFG(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, FGRenderer)
    serializer_class = SocialDemoRatingFGSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

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
        get_key = lambda x: x['title__title']
        for key, group in groupby(sorted(serializer.data, key=get_key), key=get_key):
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


class SpecialByThemePublicationSocialDemoRatingFG(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, FGRenderer)
    serializer_class = SocialDemoRatingFGSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow,
                          IsRequestsToPublicationAllow, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = self.handle_request_params(request)

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


class GeneralByThemesSocialDemoRatingFG(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, FGRenderer)
    serializer_class = SocialDemoRatingFGSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow, IsRequestsToThemeAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('title__title', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = self.handle_request_params(request)
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


class GeneralByPublicationsSocialDemoRatingFG(generics.ListAPIView, ParamsHandler):
    renderer_classes = (r.BrowsableAPIRenderer, r.JSONRenderer, FGRenderer)
    serializer_class = PublicationsSocialDemoRatingSerializer
    permission_classes = (
        permissions.IsAuthenticatedOrReadOnly, IsRequestsToSocialDemoAllow, IsRequestsToPublicationAllow)
    pagination_class = ConfiguredPageNumberPagination
    fg_values_list = ('publication', 'views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def list(self, request, *args, **kwargs):

        params = self.handle_request_params(request)
        params.pop("aggregator")

        if len(params):
            self.queryset = PublicationsSocialDemoRating.objects.filter(
                created_date__gte=params['posted_date__gte'],
                created_date__lte=params['posted_date__lte']).order_by("-views")
            if "publication__in" in params:
                self.queryset = self.queryset.filter(publication__in=params['publication__in']).order_by("-views")
        else:
            self.queryset = PublicationsSocialDemoRating.objects.all().order_by("-views")

        serializer = self.get_serializer(data=list(self.queryset.values()), many=True)
        serializer.is_valid(raise_exception=True)

        result = []
        get_key = lambda x: x['publication']
        for key, group in groupby(sorted(serializer.data, key=get_key), key=get_key):
            item = dict(zip(self.fg_values_list, [{} for i in range(len(self.fg_values_list))]))
            item['publication'] = key
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
