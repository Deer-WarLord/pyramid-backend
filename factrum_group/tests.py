# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from urllib.parse import urlparse

from django.core.files.base import ContentFile
from rest_framework import status
from rest_framework.test import APITestCase

from factrum_group.serializers import PublicationsSocialDemoRatingSerializer
from uploaders.models import *
from rest_framework.test import APIClient
from django.conf import settings
from django.urls import reverse
from factrum_group.models import *
from noksfishes.serializers import Publication, PublicationSerializer
from datetime import datetime
from os.path import abspath, dirname, join
import json


class SaveDataFromProviderTests(APITestCase):
    fixtures = ['initial_data.json']

    def setUp(self):
        self.tearDown()
        self.abspath = abspath("../../noksfishes_test_data")
        self.headers = {
            'HTTP_CONTENT_DISPOSITION': 'attachment; filename={}'.format("test_upload.json"),
        }

    def tearDown(self):
        UploadedInfo.objects.all().delete()
        Publication.objects.all().delete()
        Theme.objects.all().delete()

    def _get_data(self, path):
        f = open(join(self.abspath, path), 'rb')
        return f

    @unittest.skip("temp")
    def test_upload_file_valid_general(self):
        f = self._get_data("publications.json")
        data = json.loads(f.read())
        f.seek(0)
        upload_info = UploadedInfo.objects.create(provider=Provider.objects.get(id=10), title="test",
                                                  file=ContentFile(f.read()))
        upload_info.save()
        serializer = PublicationSerializer(data=data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save(upload_info=upload_info)

        for id, item in enumerate(data):
            theme = Theme(id=id + 1, title=item["key_word"])
            theme.save()

        self.assertEqual(1, Publication.objects.count())
        self.assertEqual(1, Theme.objects.count())

        data = {
            "title": "testCompany",
            "file": self._get_data("analyzed_info.json")
        }

        client = APIClient()
        client.login(username='factrum', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(title='factrum_group').title)
        self.assertEqual(len(AnalyzedInfo.objects.all()), 0)
        # self.assertEqual(int(list(AnalyzedInfo.objects.all())[0].article.posted_date.strftime("%s")),
        #                  int(datetime.strptime("2018-10-02", "%Y-%m-%d").strftime("%s")))
        # self.assertEqual(list(AnalyzedInfo.objects.all())[0].views, 0)

    # @unittest.skip("temp")
    def test_upload_file_valid_details(self):
        data = {
            "title": "10-2017",
            "file": self._get_data("detailed_week22_06.2018.json")
        }

        client = APIClient()
        client.login(username='factrum_social', password='test_123')

        for id in range(1, 500):
            theme = Theme(id=id, title="test name %d" % id)
            theme.save()

        self.assertEqual(499, Theme.objects.count())

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(title='factrum_group_social').title)
        self.assertEqual(len(SocialDetails.objects.all()), 130)
        details = list(SocialDetails.objects.all())[0]
        self.assertEqual(sum(details.sex.values()), 100)
        self.assertEqual(sum(details.age.values()), 101)
        self.assertEqual(sum(details.children_lt_16.values()), 100)
        self.assertEqual(sum(details.education.values()), 99)
        self.assertEqual(sum(details.group.values()), 100)
        self.assertEqual(sum(details.income.values()), 100)
        self.assertEqual(sum(details.region.values()), 100)
        self.assertEqual(sum(details.occupation.values()), 101)
        self.assertEqual(sum(details.typeNP.values()), 100)
        self.assertEqual(sum(details.marital_status.values()), 100)

    @unittest.skip("temp")
    def test_serialize_publication_rating(self):
        js_data = json.loads(self._get_data("publication_sd_rating.json").read())
        start_date = datetime.strptime("10-2017", "%m-%Y")
        for item in js_data:
            item["created_date"] = start_date

        serializer = PublicationsSocialDemoRatingSerializer(data=js_data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        self.assertEqual(PublicationsSocialDemoRating.objects.count(), 3)
