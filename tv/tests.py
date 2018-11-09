# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from urllib.parse import urlparse

from django.core.exceptions import ObjectDoesNotExist
from rest_framework import status
from rest_framework.test import APITestCase

from admixer.serializers import AnalyzedInfoSerializer
from admixer.utils import get_analyzed_info
from tv.models import TvMetrics
from uploaders.models import *
from rest_framework.test import APIClient
from django.conf import settings
from django.urls import reverse
from admixer.models import *
from os.path import abspath, dirname, join


class SaveShortTvDataTests(APITestCase):
    fixtures = ['initial_data.json']

    def setUp(self):
        self.tearDown()
        self.url = reverse('archive')
        self.abspath = abspath(dirname(__file__))

    def _get_headers(self, fname):
        return {
            'HTTP_CONTENT_DISPOSITION': 'attachment; filename={}'.format(fname),
        }

    def _get_data(self, path):
        f = open(join(self.abspath, path), 'rb')
        return f

    def tearDown(self):
        UploadedInfo.objects.all().delete()

    def test_upload_init_data_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=11).id,
            "file": self._get_data("test_data/import_tv_short.xlsx")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self._get_headers("import_tv_short.xlsx"))

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='11').id)
        self.assertEqual(len(TvMetrics.objects.all()), 5114)



