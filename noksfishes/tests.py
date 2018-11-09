# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from urllib.parse import urlparse
from rest_framework import status
from rest_framework.test import APITestCase
from rest_framework.exceptions import ValidationError
from uploaders.models import *
from rest_framework.test import APIClient
from django.conf import settings
from django.urls import reverse
from django.core.files.base import ContentFile
from noksfishes.models import *
from datetime import datetime
from os.path import abspath, dirname, join
import json
from noksfishes.serializers import ShukachResponseSerializer, PublicationSerializer, ShukachPublicationSerializer
from noksfishes.utils import get_shukach_ids, get_ids_for_urls


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

    def _get_data(self, path):
        f = open(join(self.abspath, path), 'rb')
        return f

    @unittest.skip("temp")
    def test_upload_info_valid(self):

        data = {
            "title": "testCompany",
            "file": self._get_data("analyzed_info.json")
        }

        client = APIClient()
        client.login(username='noksfishes', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(title='noksfishes').title)
        self.assertEqual(len(AnalyzedInfo.objects.all()), 2)
        self.assertEqual(int(list(AnalyzedInfo.objects.all())[0].posted_date.strftime("%s")),
                         int(datetime.strptime("13-12-2017 23:59", "%d-%m-%Y %H:%M").strftime("%s")))

    @unittest.skip("temp")
    def test_upload_publication_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("publications.json")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='10').id)
        self.assertEqual(len(Publication.objects.all()), 1)
        self.assertEqual(Publication.objects.all().first().created_date, None)

    @unittest.skip("temp")
    def test_upload_publication_tv_valid(self):

        data = {
            "title": "tv_publications_january",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("publications_tv.json")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='10').id)
        self.assertEqual(len(Publication.objects.all()), 31)
        self.assertEqual(Publication.objects.all().first().key_word, "Укртелеком")

    @unittest.skip("temp")
    def test_upload_publication_zip_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("publications.json.zip")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='10').id)
        self.assertEqual(len(Publication.objects.all()), 1)
        self.assertEqual(Publication.objects.all().first().created_date, None)

    @unittest.skip("temp")
    def test_get_exported_publication_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("publications.json.zip")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response = client.get(reverse('noksfishes:export-publications'))

        self.assertEqual(len(response.data), 1)
        self.assertSequenceEqual(['id_article', 'id_title', 'title', 'site', 'url', 'posted_date'], response.data[0].keys())

        self.assertEqual(response.data[0]['title'], "some_keyword")

    @unittest.skip("temp")
    def test_get_exported_publication_tv(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("publications_tv.json")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        response = client.get(reverse('noksfishes:export-tv-publications'))

        self.assertEqual(len(response.data), 31)
        self.assertSequenceEqual(['id_article', 'id_title', 'key_word', 'title', 'posted_date', 'posted_time',
                                  'end_time', 'publication'], response.data[0].keys())

        self.assertEqual(response.data[0]['key_word'], "Укртелеком")

    @unittest.skip("temp")
    def test_get_exported_publication_filter_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("publications_filters.json")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        query = {
            'key_word': 'Turkcell',
            'posted_date__gt': '2016-01-01',
            'posted_date__lt': '2016-01-10'
        }

        response = client.get(reverse('noksfishes:export-publications'), data=query)

        self.assertEqual(len(response.data), 1)
        self.assertSequenceEqual(['id_article', 'id_title', 'title', 'site', 'url', 'posted_date'], response.data[0].keys())

        self.assertEqual(response.data[0]['title'], "Turkcell")

    @unittest.skip("temp")
    def test_upload_init_data_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("initial_data.json")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='10').id)
        self.assertEqual(len(Publication.objects.all()), 30618)

    @unittest.skip("temp")
    def test_upload_init_data_zip_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=10).id,
            "file": self._get_data("initial_data.json.zip")
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive'), data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(id='10').id)
        self.assertEqual(len(Publication.objects.all()), 30618)

    @unittest.skip("temp")
    def test_upload_company_data_csv_valid(self):

        companies = {
            "apk": 1282,
            "banks": 24605,
            "farma": 706,
            "san": 706,
            "tek": 3863,
            "tele": 2665
        }

        total = 0

        for fname, records in companies.items():
            total += records
            data = {
                "title": "testCompany",
                "provider": Provider.objects.get(id=10).id,
                "file": self._get_data("%s.json" % fname)
            }

            client = APIClient()
            client.login(username='owner', password='test_123')

            response = client.post(reverse('archive'), data, format='multipart', **self.headers)

            self.assertEqual(response.status_code, status.HTTP_201_CREATED)

            self.assertIn('created_date', response.data)
            self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
            self.assertEqual(response.data['provider'], Provider.objects.get(id='10').id)
            self.assertEqual(len(Publication.objects.all()), total)

    # @unittest.skip("temp")
    def test_upload_shukach_publication_valid(self):

        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(id=12).id,
            "url": "https://shukach.info/data/company/result_company_11_Jun_2018_00_34_00_d9948.json"
        }

        client = APIClient()
        client.login(username='owner', password='test_123')

        response = client.post(reverse('archive-async'), data, format='multipart', **self.headers)
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(len(Publication.objects.all()), 2)


class ShukachTests(APITestCase):

    fixtures = ['initial_data.json']

    def _get_data(self, path):
        f = open(join(abspath("../../noksfishes_test_data"), path), 'rb')
        return f

    @unittest.skip("temp")
    def test_serializer_exception(self):

        # Validate empty response
        with self.assertRaises(ValidationError):
            serializer = ShukachResponseSerializer(data={})
            serializer.is_valid(raise_exception=True)

        # Validate ajx_status status
        with self.assertRaises(ValidationError):
            serializer = ShukachResponseSerializer(data={
                "data": {
                    "153211847": "http://test_url.com"
                },
                "count": 1,
                "ajx_status": "test",
                "ajx_mess": ""
            })
            serializer.is_valid(raise_exception=True)

        # Validate count and data size equals
        with self.assertRaises(ValidationError):
            serializer = ShukachResponseSerializer(data={
                "data": {
                    "153211847": "http://test_url.com"
                },
                "count": 2,
                "ajx_status": "ok",
                "ajx_mess": ""
            })
            serializer.is_valid(raise_exception=True)

    @unittest.skip("temp")
    def test_get_shukach_ids(self):
        data = get_shukach_ids(["http://from-ua.com/news/411706-v-ukrainu-prishel-novii-mobilnii-operator.html"])
        self.assertEqual(len(data), 1)

    @unittest.skip("temp")
    def test_get_ids_for_urls_empty_table(self):
        f = self._get_data("publications_get_urls.json")
        data = json.loads(f.read())
        f.seek(0)
        upload_info = UploadedInfo.objects.create(provider=Provider.objects.get(id=10), title="test", file=ContentFile(f.read()))
        upload_info.save()
        serializer = PublicationSerializer(data=data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save(upload_info=upload_info)
        get_ids_for_urls()
        expected = len(data)
        self.assertEqual(expected, ShukachPublication.objects.count())

    @unittest.skip("temp")
    def test_get_ids_for_urls_not_empty_table(self):
        f = self._get_data("publications_get_urls.json")
        data = json.loads(f.read())
        f.seek(0)
        upload_info = UploadedInfo.objects.create(provider=Provider.objects.get(id=10), title="test", file=ContentFile(f.read()))
        upload_info.save()
        serializer = PublicationSerializer(data=data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save(upload_info=upload_info)
        for publication in Publication.objects.filter(title__in=("scenario1", "scenario3")):
            item = ShukachPublication.objects.create(publication=publication, shukach_id=publication.id + 1000)
            item.save()

        spub_init = ShukachPublication.objects.all()

        self.assertEqual(2, len(spub_init))
        self.assertEqual("scenario1", spub_init[0].publication.title)
        self.assertEqual("scenario3", spub_init[1].publication.title)

        get_ids_for_urls()
        spub_updated = ShukachPublication.objects.all()
        self.assertEqual(4, len(spub_updated))
        actual = list(spub_updated.values_list("publication__title", flat=True))
        expected = ["scenario0", "scenario1", "scenario2", "scenario3"]
        expected.sort()
        actual.sort()
        self.assertEqual(expected, actual)

    # @unittest.skip("temp")
    def test_serialize_with_ids(self):
        f = self._get_data("telecom.json")
        data = json.loads(f.read())
        f.seek(0)
        upload_info = UploadedInfo.objects.create(provider=Provider.objects.get(id=12), title="test", file=ContentFile(f.read()))
        upload_info.save()
        serializer = ShukachPublicationSerializer(data=data, many=True)
        serializer.is_valid(raise_exception=True)
        serializer.save(upload_info=upload_info)

        s_res = ShukachPublication.objects.all()
        p_res = Publication.objects.all()

        self.assertEqual(8180, len(s_res))
        self.assertEqual(8180, len(p_res))






