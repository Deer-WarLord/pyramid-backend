# coding=utf-8
import calendar
from datetime import datetime, timedelta
from rest_framework import serializers
from rest_framework.exceptions import ValidationError

from noksfishes.models import Theme


class ThemeCompanyRatingSerializer(serializers.Serializer):
    key_word = serializers.CharField(max_length=1024)
    publication_amount = serializers.IntegerField()
    date = serializers.DateField(required=False)

    def to_internal_value(self, data):

        year = data.pop("year")
        week = data.pop("week")
        data["date"] = datetime.strptime("%d %d 0" % (year, week), "%Y %W %w").strftime("%Y-%m-%d")
        return super(ThemeCompanyRatingSerializer, self).to_internal_value(data=data)


class ObjectCompanyRatingSerializer(serializers.Serializer):
    object = serializers.CharField(max_length=1024)
    publication_amount = serializers.IntegerField()
    date = serializers.DateField(required=False)

    def to_internal_value(self, data):

        year = data.pop("year")
        week = data.pop("week")
        data["date"] = datetime.strptime("%d %d 0" % (year, week), "%Y %W %w").strftime("%Y-%m-%d")
        return super(ObjectCompanyRatingSerializer, self).to_internal_value(data=data)


class ThemeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Theme
        fields = '__all__'


class ThemeCompanyViewsSerializer(serializers.Serializer):
    key_word = serializers.CharField(max_length=1024)
    views = serializers.IntegerField()
    date = serializers.DateField(required=False)

    def to_internal_value(self, data):
        date = data.pop("upload_info__title")
        try:
            start_period = datetime.strptime(date, "%m-%Y")
            end_period = start_period + timedelta(calendar.monthrange(start_period.year, start_period.month)[1])
        except ValueError:
            try:
                start_period = datetime.strptime(date, "%w-%W-%Y")
                end_period = start_period + timedelta(days=6)
            except ValueError:
                raise ValidationError({"title": ["Title should be in format %m-%Y or %w-%W-%Y"]})

        data["date"] = end_period.strftime("%Y-%m-%d")
        data["key_word"] = data.pop("title__title")
        return super(ThemeCompanyViewsSerializer, self).to_internal_value(data=data)


class ObjectViewsSerializerAdmixer(serializers.Serializer):
    object = serializers.CharField(max_length=1024)
    views = serializers.IntegerField()
    date = serializers.DateField(required=False)


class ObjectViewsSerializerFG(ObjectViewsSerializerAdmixer):

    def to_internal_value(self, data):
        date = data.pop("upload_info__title")
        try:
            start_period = datetime.strptime(date, "%m-%Y")
            end_period = start_period + timedelta(calendar.monthrange(start_period.year, start_period.month)[1])
        except ValueError:
            try:
                start_period = datetime.strptime(date, "%w-%W-%Y")
                end_period = start_period + timedelta(days=6)
            except ValueError:
                raise ValidationError({"title": ["Title should be in format %m-%Y or %w-%W-%Y"]})

        data["date"] = end_period.strftime("%Y-%m-%d")
        data["object"] = data.pop("article__object")
        return super(ObjectViewsSerializerFG, self).to_internal_value(data=data)


class ThemeCompanySdViewsSerializer(serializers.Serializer):
    key_word = serializers.CharField(max_length=1024)
    views = serializers.IntegerField()
    date = serializers.DateField(required=False)
    sex = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    age = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    education = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    children_lt_16 = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    marital_status = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    occupation = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    group = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    income = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    region = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    typeNP = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))

    def to_internal_value(self, data):
        date = data.pop("upload_info__title")
        try:
            start_period = datetime.strptime(date, "%m-%Y")
            end_period = start_period + timedelta(calendar.monthrange(start_period.year, start_period.month)[1])
        except ValueError:
            try:
                start_period = datetime.strptime(date, "%w-%W-%Y")
                end_period = start_period + timedelta(days=6)
            except ValueError:
                raise ValidationError({"title": ["Title should be in format %m-%Y or %w-%W-%Y"]})

        data["date"] = end_period.strftime("%Y-%m-%d")
        data["key_word"] = data.pop("title__title")
        return super(ThemeCompanySdViewsSerializer, self).to_internal_value(data=data)


class ObjectSdViewsSerializer(serializers.Serializer):
    object = serializers.CharField(max_length=1024)
    views = serializers.IntegerField()
    date = serializers.DateField(required=False)
    sex = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    age = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    education = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    children_lt_16 = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    marital_status = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    occupation = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    group = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    income = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    region = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    typeNP = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))

    def to_internal_value(self, data):
        date = data.pop("upload_info__title")
        try:
            start_period = datetime.strptime(date, "%m-%Y")
            end_period = start_period + timedelta(calendar.monthrange(start_period.year, start_period.month)[1])
        except ValueError:
            try:
                start_period = datetime.strptime(date, "%w-%W-%Y")
                end_period = start_period + timedelta(days=6)
            except ValueError:
                raise ValidationError({"title": ["Title should be in format %m-%Y or %w-%W-%Y"]})

        data["date"] = end_period.strftime("%Y-%m-%d")
        data["key_word"] = data.pop("title__title")
        return super(ObjectSdViewsSerializer, self).to_internal_value(data=data)


