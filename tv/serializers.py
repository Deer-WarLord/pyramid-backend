# coding=utf-8
from datetime import datetime
from rest_framework import serializers
from rest_framework.serializers import LIST_SERIALIZER_KWARGS, ListSerializer

from noksfishes.models import Publication
from tv.models import TvMetrics


# class TvChannelInfoSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = TvChannelInfo
#         fields = '__all__'
#
#     @classmethod
#     def many_init(cls, *args, **kwargs):
#         allow_empty = kwargs.pop('allow_empty', None)
#         data = kwargs.pop("data")
#         kwargs["data"] = [data] # TODO check for multiple
#         child_serializer = cls(*args, **kwargs)
#         list_kwargs = {
#             'child': child_serializer,
#         }
#         if allow_empty is not None:
#             list_kwargs['allow_empty'] = allow_empty
#         list_kwargs.update({
#             key: value for key, value in kwargs.items()
#             if key in LIST_SERIALIZER_KWARGS
#         })
#         meta = getattr(cls, 'Meta', None)
#         list_serializer_class = getattr(meta, 'list_serializer_class', ListSerializer)
#         return list_serializer_class(*args, **list_kwargs)
#
#     def to_internal_value(self, data):
#         internal_data = {
#             "posted_date": data[0][u"Дата"],
#             "channel": data[0][u"Канал"],
#             "genre": data[0][u"Жанр"],
#             "media_group": data[0][u"Медиагруппа"],
#             "time_rate": dict([(item[u"Временной диапазон"], float(item[u"Rat%"].replace(',','.'))) for item in data])
#         }
#         internal_data["time_rate"]["total"] = internal_data["time_rate"].pop('')
#
#         return super(TvChannelInfoSerializer, self).to_internal_value(internal_data)
#
#     upload_info = serializers.ReadOnlyField()
#     time_rate = serializers.DictField()


class PublicationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publication
        fields = ["key_word", "title", "posted_date", "posted_time", "end_time", "publication"]

    def _standartize_time(self, time_str):
        hours, rest = time_str.split(":", 1)
        hours = ''.join(i for i in hours if i.isdigit())
        if int(hours) > 23:
            hours = "0%d" % (int(hours) - 24)
        return hours + ":" + rest

    def to_internal_value(self, data):

        try:
            data["end_time"] = self._standartize_time(str(data["end_time"]))
            datetime.strptime(data["end_time"], "%H:%M:%S")
        except Exception as e:
            data["end_time"] = "00:00:00"

        try:
            datetime.strptime(data["posted_time"], "%H:%M:%S")
        except Exception as e:
            data["posted_time"] = "00:00:00"

        return super(PublicationSerializer, self).to_internal_value(data)


class TvMetricsSerializer(serializers.ModelSerializer):
    class Meta:
        model = TvMetrics
        fields = '__all__'

    upload_info = serializers.ReadOnlyField()
    publication = PublicationSerializer(required=True)
    rat = serializers.FloatField(required=False, allow_null=True)
    shr = serializers.FloatField(required=False, allow_null=True)

    def to_internal_value(self, data):
        try:
            data["rat"] = float(data["rat"])
        except Exception as e:
            data["rat"] = 0.0

        try:
            data["shr"] = float(data["shr"])
        except Exception as e:
            data["shr"] = 0.0

        return super(TvMetricsSerializer, self).to_internal_value(data)

    def create(self, validated_data):
        publication_data = validated_data.pop('publication')
        publication = PublicationSerializer.create(PublicationSerializer(), validated_data=publication_data)
        tv, created = TvMetrics.objects.update_or_create(publication=publication,
                                                         upload_info=validated_data.pop('upload_info'),
                                                         rat=validated_data.pop('rat'),
                                                         shr=validated_data.pop('shr'))
        return tv