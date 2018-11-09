# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import unittest
from urllib.parse import urlparse
from rest_framework import status
from rest_framework.exceptions import ValidationError
from rest_framework.test import APITestCase
from uploaders.models import *
from rest_framework.test import APIClient
from django.conf import settings
from django.urls import reverse
from django.contrib.auth.models import Group


class MockSerializer:
    def __init__(self, *args, **kwargs):
        self.data = kwargs['data']

    def is_valid(self, raise_exception=False):
        if not len(self.data) > 0:
            raise ValidationError({})
        return True

    def save(self, **kwargs):
        pass


class FileUploadTests(APITestCase):
    def setUp(self):
        self.tearDown()
        parser_info = {"path": "uploaders.tests", "class": "MockSerializer"}
        provider = Provider(title="test", description="testDescription", parser_info=parser_info)
        provider.save()

        user = User.objects.create_superuser('test', 'test@test.test', 'test')
        user.provider = provider
        user.save()

        gr, created = Group.objects.get_or_create(name='Owners')
        user = User.objects.create_user("test2", "test2@test2.test2", "test2")
        user.save()
        user.groups.add(gr)

        self.url = reverse('archive')
        self.filename = "test_upload.json"
        self.path = '/tmp/%s' % self.filename
        self.headers = {
            'HTTP_CONTENT_DISPOSITION': 'attachment; filename={}'.format(self.filename),
        }

    def tearDown(self):
        UploadedInfo.objects.all().delete()
        User.objects.all().delete()
        Provider.objects.all().delete()

    def _create_invalid_test_file(self):
        f = open(self.path, 'w')
        f.write("test123\n")
        f.close()
        f = open(self.path, 'rb')
        return f

    def _create_valid_test_file(self):
        f = open(self.path, 'w')
        f.write('{"test1": 1,"test2": 2,"test3": 3}')
        f.close()
        f = open(self.path, 'rb')
        return f

    # @unittest.skip("temp")
    def test_upload_file_unauthorized(self):
        data = {}
        client = APIClient()
        response = client.post(self.url, data, format='multipart', **self.headers)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    # @unittest.skip("temp")
    def test_upload_file_invalid(self):
        data = {
            "title": "testCompany",
            "file": self._create_invalid_test_file()
        }

        client = APIClient()
        client.login(username='test', password='test')

        response = client.post(self.url, data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    # @unittest.skip("temp")
    def test_upload_file_valid(self):
        data = {
            "title": "testCompany",
            "file": self._create_valid_test_file()
        }

        client = APIClient()
        client.login(username='test', password='test')

        response = client.post(self.url, data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(title='test').title)

    # @unittest.skip("temp")
    def test_upload_file_owner_invalid(self):
        data = {
            "title": "testCompany",
            "file": self._create_valid_test_file()
        }

        client = APIClient()
        client.login(username='test2', password='test2')

        response = client.post(self.url, data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    # @unittest.skip("temp")
    def test_upload_file_owner_valid(self):
        data = {
            "title": "testCompany",
            "provider": Provider.objects.get(title="test").id,
            "file": self._create_valid_test_file()
        }

        client = APIClient()
        client.login(username='test2', password='test2')

        response = client.post(self.url, data, format='multipart', **self.headers)

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        self.assertIn('created_date', response.data)
        self.assertTrue(urlparse(response.data['file']).path.startswith(settings.MEDIA_URL))
        self.assertEqual(response.data['provider'], Provider.objects.get(title='test').id)
