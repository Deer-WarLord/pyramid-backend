# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from urllib.parse import urlparse

from clickhouse_driver import Client
from django.core.exceptions import ObjectDoesNotExist
from rest_framework import status
from rest_framework.test import APITestCase

from admixer.serializers import AnalyzedInfoSerializer, DynamicAnalyzedInfoSerializer
from admixer.utils import get_analyzed_info, save_admixer_data
from uploaders.models import *
from rest_framework.test import APIClient
from django.conf import settings
from django.urls import reverse
from admixer.models import *
from os.path import abspath, dirname, join
import json
import csv


class SaveDataFromProviderTests(APITestCase):
    fixtures = ['initial_data.json']

    def setUp(self):
        self.tearDown()
        self.url = reverse('archive')
        self.filename = "test_upload.csv"
        self.path = '/tmp/%s' % self.filename
        self.abspath = abspath(dirname(__file__))
        self.headers = {
            'HTTP_CONTENT_DISPOSITION': 'attachment; filename={}'.format(self.filename),
        }

        self.data = [
            {
                "url_id": 150959918,
                "platform": 19,
                "browser": 5,
                "region": "UA/05/702320",
                "age": 5,
                "gender": 1,
                "income": 1,
                "uniques": 1,
                "views": 2
            },
            {
                "url_id": 150879956,
                "platform": 21,
                "browser": 2,
                "region": "UA/12",
                "age": 5,
                "gender": 1,
                "income": 2,
                "uniques": 1,
                "views": 1
            }
        ]

    def tearDown(self):
        UploadedInfo.objects.all().delete()

    def _create_valid_test_file(self):
        with open(self.path, 'w') as csvfile:
            fieldnames = ('url_id', 'platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views')
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.data:
                writer.writerow(row)

        f = open(self.path, 'r')
        return f

    def _get_data(self, path):
        f = open(join(self.abspath, path), 'r')
        return f

    @unittest.skip("temp")
    def test_upload_file_valid(self):
        data = {
            "title": "testCompany",
            "file": self._create_valid_test_file()
        }

        client = APIClient()
        client.login(username='admixer', password='test_123')

        response = client.post(self.url, data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(title='admixer').title)
        self.assertEqual(len(AnalyzedInfo.objects.all()), 2)

    @unittest.skip("temp")
    def test_upload_init_data_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=2).id,
            "file": self._get_data("test_data/analyzed_info.csv")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='2').id)
        self.assertEqual(len(AnalyzedInfo.objects.all()), 241424)

    @unittest.skip("temp")
    def test_upload_init_data_zip_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=2).id,
            "file": self._get_data("test_data/analyzed_info.csv.zip")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='2').id)
        self.assertEqual(len(AnalyzedInfo.objects.all()), 241424)

    @unittest.skip("temp")
    def test_click_house_request(self):

        self.assertEqual(0, len(get_analyzed_info([])))

        save_admixer_data([155429033])
        self.assertTrue(AnalyzedInfo.objects.count() >= 2794)

        save_admixer_data([155429033, 153651138, 155739699])
        self.assertTrue(AnalyzedInfo.objects.count() >= 5828)

    def test_click_house_filtered_request(self):

        shukach_ids = [155429033]

        client = Client(settings.CLICKHOUSE_HOST,
                        database=settings.CLICKHOUSE_DB,
                        user=settings.CLICKHOUSE_USER,
                        password=settings.CLICKHOUSE_PASSWORD)

        query = 'select UrlId, Platform, Browser, Country, Age, Gender, Income, count(distinct IntVisKey), Sum(Views), Date ' \
                'from admixer.UrlStat ' \
                'where UrlId in (%s) and Date >= \'2018-01-01\' and Date <= \'2018-09-01\'' \
                'Group by UrlId, Platform, Browser, Country, Age, Gender, Income, Date' % (",".join("'%d'" % item for item in shukach_ids))

        response = client.execute(query)
        keys = ('url_id', 'platform', 'browser', 'region', 'age', 'gender', 'income', 'uniques', 'views', 'date')
        results = []

        for row in response:
            item = dict(zip(keys, row))
            item['url_id'] = int(row[0])
            results.append(item)

        serializer = DynamicAnalyzedInfoSerializer(data=results, many=True)
        serializer.is_valid(raise_exception=False)

        self.assertEqual(len(serializer.data), 22)








