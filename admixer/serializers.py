from rest_framework import serializers
from admixer.models import *


class AnalyzedInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalyzedInfo
        fields = '__all__'

    upload_info = serializers.ReadOnlyField()


class DynamicAnalyzedInfoSerializer(serializers.Serializer):
    key_word = models.CharField(max_length=1024)
    platform = models.IntegerField(blank=True, null=True)
    browser = models.IntegerField(blank=True, null=True)
    region = models.CharField(max_length=256, default="UA", blank=True, null=True)
    age = models.IntegerField(blank=True, null=True)
    gender = models.IntegerField(blank=True, null=True)
    income = models.IntegerField(blank=True, null=True)
    uniques = models.IntegerField(blank=True, null=True)
    views = models.IntegerField()
    date = models.DateField()