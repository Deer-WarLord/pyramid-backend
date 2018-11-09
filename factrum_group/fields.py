from rest_framework import serializers
from django.utils.translation import ugettext_lazy as _


class CustomDictField(serializers.DictField):

    def __init__(self, *args, **kwargs):
        self.keys = kwargs.pop('keys', [])
        super(CustomDictField, self).__init__(*args, **kwargs)
        self.default_error_messages.update({
            'wrong_key': _('Wrong keys. Expected %s' % self.keys),
            'wrong_sum': _('Total sum is {total_sum} but should be 100. Title id is {title}')
        })

    def to_representation(self, obj):
        return str(obj)

    def to_internal_value(self, data):
        parsed_data = super(CustomDictField, self).to_internal_value(data)

        if len(self.keys) and not all(key in self.keys for key in parsed_data.keys()):
            self.fail("wrong_key")
        # total_sum = sum(parsed_data.values())
        # if not total_sum == 100 and not total_sum == 0:
        #     self.fail("wrong_sum", total_sum = total_sum, title=self.parent.internal_data['title'])
        return parsed_data