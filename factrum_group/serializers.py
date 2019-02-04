from rest_framework.exceptions import ValidationError

from factrum_group.models import *
from factrum_group.fields import *
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import Count
from noksfishes.models import Publication

import copy
import datetime
import calendar
from uploaders.utils import URLField

import logging

logger = logging.getLogger(__name__)


class AnalyzedInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalyzedInfo
        fields = '__all__'

    upload_info = serializers.ReadOnlyField()

    def __new__(cls, *args, **kwargs):
        if kwargs.pop('many', False):
            filtered_data = []
            for raw in kwargs["data"]:
                try:
                    Publication.objects.get(id=raw["id_article"])
                except Publication.DoesNotExist:
                    pass
                else:
                    filtered_data.append(raw)
            logger.info("Omitted %d non-existent ids", len(kwargs["data"]) - len(filtered_data))
            kwargs["data"] = filtered_data
            return cls.many_init(*args, **kwargs)
        return super(AnalyzedInfoSerializer, cls).__new__(cls, *args, **kwargs)

    def to_internal_value(self, data):
        internal_data = {'article': int(data.pop("id_article")) if "id_article" in data else None,
                         'title': int(data.pop("id_title")) if "id_title" in data else None,
                         'views': data.pop("views") if "views" in data else None}
        return super(AnalyzedInfoSerializer, self).to_internal_value(internal_data)


class AnalyzedInfoGeneralSerializer(serializers.Serializer):
    theme = serializers.CharField()
    views = serializers.IntegerField()


class AnalyzedInfoDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalyzedInfo
        fields = ['theme', 'url', 'publication', 'date', 'views']

    theme = serializers.CharField(source='title.title')
    url = serializers.CharField(source='article.url')
    publication = serializers.CharField(source='article.publication')
    date = serializers.CharField(source='article.posted_date')
    views = serializers.IntegerField()


class ExportPublicationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publication
        fields = ['id_article', 'id_title', 'title', 'site', 'url', 'posted_date']

    id_article = serializers.IntegerField(source='id')  # TODO Make external ID for Publication.id
    id_title = serializers.SerializerMethodField()
    title = serializers.CharField(source='key_word')
    site = serializers.CharField(source='title')
    url = URLField(max_length=1024)

    def get_id_title(self, obj):
        return Theme.objects.get_or_create(title=obj.key_word)[0].id


class PublicationsSocialDemoRatingSerializer(serializers.ModelSerializer):
    class Meta:
        model = PublicationsSocialDemoRating
        fields = '__all__'
    sex = serializers.DictField(required=False, child=serializers.IntegerField(min_value=0))
    age = serializers.DictField(child=serializers.IntegerField(min_value=0))
    education = serializers.DictField(child=serializers.IntegerField(min_value=0))
    children_lt_16 = serializers.DictField(child=serializers.IntegerField(min_value=0))
    marital_status = serializers.DictField(child=serializers.IntegerField(min_value=0))
    occupation = serializers.DictField(child=serializers.IntegerField(min_value=0))
    group = serializers.DictField(child=serializers.IntegerField(min_value=0))
    income = serializers.DictField(child=serializers.IntegerField(min_value=0))
    region = serializers.DictField(child=serializers.IntegerField(min_value=0))
    typeNP = serializers.DictField(child=serializers.IntegerField(min_value=0))


class GeneratePublicationsSocialDemoRating:
    def __init__(self):
        pass

    fg_values_list = ('views', 'sex', 'age', 'education', 'children_lt_16',
                      'marital_status', 'occupation', 'group', 'income', 'region', 'typeNP')

    def concat_dict(self, ld, rd):
        return dict([(k, ld[k] + rd[k] if k in rd else 0) for k in ld.keys()])

    def transform_concat(self, total, sd, all, specific):
        views = sd["views"] / all * specific
        calculate = lambda v: v / 100.0 * views
        converter = lambda d: dict([(k, int(calculate(v))) for k, v in d.items()])
        return {
            "views": int(total["views"] + views),
            "sex": self.concat_dict(total["sex"], converter(sd["sex"])),
            "age": self.concat_dict(total["age"], converter(sd["age"])),
            "education": self.concat_dict(total["education"], converter(sd["education"])),
            "children_lt_16": self.concat_dict(total["children_lt_16"], converter(sd["children_lt_16"])),
            "marital_status": self.concat_dict(total["marital_status"], converter(sd["marital_status"])),
            "occupation": self.concat_dict(total["occupation"], converter(sd["occupation"])),
            "group": self.concat_dict(total["group"], converter(sd["group"])),
            "income": self.concat_dict(total["income"], converter(sd["income"])),
            "region": self.concat_dict(total["region"], converter(sd["region"])),
            "typeNP": self.concat_dict(total["typeNP"], converter(sd["typeNP"]))
        }

    def __call__(self, instances):
        logger.info("Received %d SocialDetails instances", len(instances))

        fg_map = {}
        logger.info("Making a map of SocialDetails instances")

        for obj in instances:
            fg_map[obj.title.title] = dict(
                zip(
                    self.fg_values_list,
                    [getattr(obj, field) for field in self.fg_values_list]
                )
            )

        try:
            start_date = datetime.datetime.strptime(instances[0].upload_info.title, "%m-%Y")
            end_date = start_date + datetime.timedelta(calendar.monthrange(start_date.year, start_date.month)[1])
        except ValueError as e0:
            try:
                start_date = datetime.datetime.strptime(instances[0].upload_info.title, "%w-%W-%Y")
                end_date = start_date + datetime.timedelta(days=6)
            except ValueError as e1:
                raise ValidationError({"title": ["Title should be in format %m-%Y"]})

        query_params = {
            'posted_date__gte': start_date,
            'posted_date__lte': end_date
        }

        logger.info("Building a dictionary of themes/companies rating")
        themes_rating = dict(Publication.objects.filter(**query_params).values_list(
            "key_word").annotate(publication_amount=Count("key_word")).order_by("-publication_amount"))
        logger.info("%d - keys processed" % len(themes_rating))

        logger.info("Building a dictionary of a themes/companies list for publications")
        themes_in_publication = dict(Publication.objects.filter(**query_params).values_list(
            "publication").annotate(themes=ArrayAgg("key_word")))
        logger.info("%d - keys processed" % len(themes_in_publication))

        results = []
        init_sd = {}
        info = SocialDetailsSerializer()
        for key in self.fg_values_list[1:]:
            keys = [str(f) for f in info.fields[key].keys]
            init_sd[key] = dict(zip(keys, [0] * len(keys)))
        init_sd["views"] = 0

        logger.info("Building publication social demo rating")
        i = 1
        for publication, themes in themes_in_publication.items():
            sd = copy.deepcopy(init_sd)
            for theme in set(themes):
                if theme in fg_map:
                    specific = Publication.objects.filter(key_word=theme, publication=publication,
                                                          **query_params).count()
                    sd = self.transform_concat(sd, fg_map[theme], themes_rating[theme], specific)
            sd["publication"] = publication
            sd["created_date"] = start_date
            results.append(sd)
            if i % 100 == 0:
                logger.info("%d - publications processed" % i)
            i += 1

        logger.info("%d - publications processed" % len(results))
        if len(results):
            logger.info("Serializing and validating the results")
            logger.info([item["views"] for item in results])
            serializer = PublicationsSocialDemoRatingSerializer(data=results, many=True)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            logger.info("Publication social demo rating is saved")


class SocialDetailsListSerializer(serializers.ListSerializer):
    def create(self, validated_data):
        instances = super(SocialDetailsListSerializer, self).create(validated_data)
        GeneratePublicationsSocialDemoRating()(instances)
        return instances


class SocialDetailsSerializer(serializers.ModelSerializer):
    class Meta:
        model = SocialDetails
        fields = '__all__'

    upload_info = serializers.ReadOnlyField()
    sex = CustomDictField(
        required=False,
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["male", "female"]
    )
    age = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["15-17", "18-24", "25-34", "35-44", "45+"]
    )
    education = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["lte9", "11", "bachelor", "master"]
    )
    children_lt_16 = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["yes", "no"]
    )
    marital_status = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["single", "married", "widow(er)", "divorced", "liveTogether"]
    )
    occupation = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["businessOwner", "entrepreneur", "hiredManager", "middleManager", "masterDegreeSpecialist", "employee",
              "skilledWorker", "otherWorkers", "mobileWorker", "militaryPoliceman", "student", "pensioner",
              "disabled", "housewife", "maternityLeave", "temporarilyUnemployed", "other"]
    )
    group = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["1", "2", "3", "4", "5"]
    )
    income = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["noAnswer", "0-1000", "1001-2000", "2001-3000", "3001-4000", "4001-5000", "gt5001"]
    )
    region = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["west", "center", "east", "south"]
    )
    typeNP = CustomDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["50+", "50-"]
    )

    def to_representation(self, data):
        representation_data = data
        title = type('', (), {})()
        title.pk = data['id_title']
        representation_data['title'] = title
        return super(SocialDetailsSerializer, self).to_representation(representation_data)

    def to_internal_value(self, data):
        internal_data = {'title': data["id_title"] if "id_title" in data else "",
                         'views': data["views"] if "views" in data else ""}
        fields = ['sex', 'age', 'education', 'children_lt_16', 'marital_status', 'occupation', 'group', 'income',
                  'region', 'typeNP']
        for field in fields:
            internal_data[field] = data[field] if field in data else {}
        self.internal_data = internal_data
        return super(SocialDetailsSerializer, self).to_internal_value(internal_data)

    @classmethod
    def many_init(cls, *args, **kwargs):
        kwargs['child'] = cls()
        return SocialDetailsListSerializer(*args, **kwargs)
