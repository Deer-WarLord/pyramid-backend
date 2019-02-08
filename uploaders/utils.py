import importlib
import datetime
from django.conf import settings
from django.core import mail
from django.contrib.auth.models import Group
from django.core.exceptions import ValidationError as DjangoValidationError
from django.utils.dateparse import parse_datetime
from django.utils.translation import ugettext_lazy as _
from rest_framework import serializers
from rest_framework.fields import get_error_detail
from rest_framework import ISO_8601
from rest_framework.exceptions import ValidationError
from rest_framework.settings import api_settings
from rest_framework.utils import humanize_datetime

import logging
logger = logging.getLogger(__name__)

DEFAULT_REGISTRY = (
    "get_queryset",
    "get_serializer_class",
    "perform_create",
    "perform_update",
    "perform_destroy",
)


def get_default_groups():
    return [group.name.lower() for group in Group.objects.all()]


class RoleError(Exception):
    """Base class for exceptions in this module."""
    pass


class RoleViewSetMixin(object):
    """A ViewSet mixin that parameterizes DRF methods over roles"""
    _viewset_method_registry = set(getattr(settings, "VIEWSET_METHOD_REGISTRY", DEFAULT_REGISTRY))
    _role_groups = set(getattr(settings, "ROLE_GROUPS", get_default_groups))

    def _call_role_fn(self, fn, *args, **kwargs):
        """Attempts to call a role-scoped method"""
        try:
            role_name = self._get_role(self.request.user)
            role_fn = "{}_for_{}".format(fn, role_name)
            return getattr(self, role_fn)(*args, **kwargs)
        except (AttributeError, RoleError):
            try:
                return getattr(self, "{}_default".format(fn))(*args, **kwargs)
            except AttributeError:
                return getattr(super(RoleViewSetMixin, self), fn)(*args, **kwargs)

    def _get_role(self, user):
        """Retrieves the given user's role"""
        user_groups = set([group.name.lower() for group in user.groups.all()])
        user_role = self._role_groups.intersection(user_groups)

        if len(user_role) < 1:
            raise RoleError("The user is not a member of any role groups")
        elif len(user_role) > 1:
            raise RoleError("The user is a member of multiple role groups")
        else:
            return user_role.pop()


def register_fn(fn):
    """Dynamically adds fn to RoleViewSetMixin"""
    def inner(self, *args, **kwargs):
        return self._call_role_fn(fn, *args, **kwargs)
    setattr(RoleViewSetMixin, fn, inner)

# Registers whitelist of ViewSet fns to override
for fn in RoleViewSetMixin._viewset_method_registry:
    register_fn(fn)


class DateTimeField(serializers.DateTimeField):

    def __init__(self, *args, **kwargs):
        self.suppress = kwargs.pop("suppress", False)
        super(DateTimeField, self).__init__(*args, **kwargs)
        self.default_error_messages['invalid'] = _('Datetime "{value}" has wrong format. Use one of these formats instead: {format}.')

    def to_internal_value(self, value):
        input_formats = getattr(self, 'input_formats', api_settings.DATETIME_INPUT_FORMATS)

        if value == "" and self.allow_null:
            return None

        if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
            self.fail('date')

        if isinstance(value, datetime.datetime):
            return self.enforce_timezone(value)

        for input_format in input_formats:
            if input_format.lower() == ISO_8601:
                try:
                    parsed = parse_datetime(value)
                    if parsed is not None:
                        return self.enforce_timezone(parsed)
                except (ValueError, TypeError):
                    pass
            else:
                try:
                    parsed = self.datetime_parser(value, input_format)
                    return self.enforce_timezone(parsed)
                except (ValueError, TypeError):
                    pass
        if self.suppress:
            return None
        humanized_format = humanize_datetime.datetime_formats(input_formats)
        self.fail('invalid', format=humanized_format, value=value)


class URLField(serializers.CharField):

    def __init__(self, **kwargs):
        super(URLField, self).__init__(**kwargs)
        # validator = URLValidator(message=self.error_messages['invalid'])
        # self.validators.append(validator)

    def run_validators(self, value):
        """
        Test the given value against all the validators on the field,
        and either raise a `ValidationError` or simply return.
        """
        errors = []
        for validator in self.validators:
            if hasattr(validator, 'set_context'):
                validator.set_context(self)

            try:
                validator(value)
            except ValidationError as exc:
                # If the validation error contains a mapping of fields to
                # errors then simply raise it immediately rather than
                # attempting to accumulate a list of errors.
                if isinstance(exc.detail, dict):
                    raise
                errors.extend("{} {}".format(exc.detail, value))
            except DjangoValidationError as exc:
                errors.extend([msg + " " + value for msg in get_error_detail(exc)])
        if errors:
            raise ValidationError(errors)


def save_data_from_provider(upload_info, json_data):
    serializer_class = getattr(importlib.import_module(upload_info.provider.parser_info["path"]),
                               upload_info.provider.parser_info["class"])
    serializer = serializer_class(data=json_data, many=True)
    serializer.is_valid(raise_exception=True)
    serializer.save(upload_info=upload_info)
    logger.info("Publications saved after from uploaded file")
    mail.send_mail('Автоматическое письмо. Pyramid. Загрузка данных',
                   'Данные из файла %s.json успешно загружены на сервре Pyramid' % upload_info.title,
                   'pyramid@gmail.com', ['deerwarlord@gmail.com', 'olepole2009@gmail.com'], fail_silently=False)
