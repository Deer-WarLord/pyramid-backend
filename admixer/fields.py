from rest_framework import serializers
from django.utils.translation import ugettext_lazy as _


class CustomDictField(serializers.DictField):

    def to_representation(self, obj):
        return obj