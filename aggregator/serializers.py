# coding=utf-8
from rest_framework import serializers

from factrum_group.fields import CustomDictField as FactrumDictField
from admixer.fields import CustomDictField as AdmixerDictField


class FactrumAdmixerGeneralSerializer(serializers.Serializer):
    shukach_id = serializers.IntegerField()
    url = serializers.CharField(max_length=1024)
    factrum_views = serializers.IntegerField()
    admixer_views = serializers.IntegerField()


class FactrumAdmixerSocialDetailsSerializer(serializers.Serializer):
    id_theme = serializers.IntegerField()
    theme = serializers.CharField(max_length=1024)
    factrum_views = serializers.IntegerField()
    admixer_views = serializers.IntegerField()
    uniques_admixer = serializers.IntegerField()

    sex_factrum = FactrumDictField(
        required=False,
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["male", "female"]
    )

    sex_admixer = AdmixerDictField()

    age_factrum = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["15-17", "18-24", "25-34", "35-44", "45+"]
    )

    age_admixer = AdmixerDictField()

    region_factrum = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["west", "center", "east", "south"]
    )

    region_admixer = AdmixerDictField()

    income_factrum = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["noAnswer", "0-1000", "1001-2000", "2001-3000", "3001-4000", "4001-5000", "gt5001"]
    )

    income_admixer = AdmixerDictField()

    education = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["lte9", "11", "bachelor", "master"]
    )

    children_lt_16 = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["yes", "no"]
    )

    marital_status = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["single", "married", "widow(er)", "divorced", "liveTogether"]
    )

    occupation = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["businessOwner", "entrepreneur", "hiredManager", "middleManager", "masterDegreeSpecialist", "employee",
              "skilledWorker", "otherWorkers", "mobileWorker", "militaryPoliceman", "student", "pensioner",
              "disabled", "housewife", "maternityLeave", "temporarilyUnemployed", "other"]
    )

    group = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["1", "2", "3", "4", "5"]
    )

    typeNP = FactrumDictField(
        child=serializers.IntegerField(min_value=0, max_value=100),
        keys=["50+", "50-"]
    )

    platform = AdmixerDictField()

    browser = AdmixerDictField()


class MarketsRatingSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField(max_length=1024)
    publication_amount = serializers.IntegerField()


class ThemeCompanyRatingSerializer(serializers.Serializer):
    key_word = serializers.CharField(max_length=1024)
    publication_amount = serializers.IntegerField()


class PublicationRatingSerializer(serializers.Serializer):
    publication = serializers.CharField(max_length=1024)
    publication_amount = serializers.IntegerField()
    country = serializers.CharField(max_length=255)
    region = serializers.CharField(max_length=255)
    city = serializers.CharField(max_length=255)
    type = serializers.CharField(max_length=255)
    topic = serializers.CharField(max_length=255)
    consolidated_type = serializers.CharField(max_length=255)


class SocialDemoRatingAdmixerSerializer(serializers.Serializer):
    aggregator = serializers.CharField(max_length=1024)
    uniques = serializers.IntegerField()
    views = serializers.IntegerField()
    age_groups = AdmixerDictField()
    income_groups = AdmixerDictField()
    gender_groups = AdmixerDictField()
    regions = AdmixerDictField()
    platforms = AdmixerDictField()
    browsers = AdmixerDictField()


class SocialDemoRatingFGSerializer(serializers.Serializer):
    title__title = serializers.CharField(max_length=1024)
    views = serializers.IntegerField()
    sex = serializers.DictField()
    age = serializers.DictField()
    education = serializers.DictField()
    children_lt_16 = serializers.DictField()
    marital_status = serializers.DictField()
    occupation = serializers.DictField()
    group = serializers.DictField()
    income = serializers.DictField()
    region = serializers.DictField()
    typeNP = serializers.DictField()

    def to_representation(self, obj):
        views = obj.get("views")

        if self.context["format"] and "all" in self.context["format"] and "specific" in self.context["format"]:
            all = self.context["format"]["all"]
            specific = self.context["format"]["specific"]
            views = obj.get("views") / all * specific

        calculate = lambda v: v / 100.0 * views

        converter = lambda d: dict([(k, int(calculate(v))) for k, v in d.items()])

        return {
            "title__title": obj.get("title__title"),
            "views": int(views),
            "sex": converter(obj.get("sex")),
            "age": converter(obj.get("age")),
            "education": converter(obj.get("education")),
            "children_lt_16": converter(obj.get("children_lt_16")),
            "marital_status": converter(obj.get("marital_status")),
            "occupation": converter(obj.get("occupation")),
            "group": converter(obj.get("group")),
            "income": converter(obj.get("income")),
            "region": converter(obj.get("region")),
            "typeNP": converter(obj.get("typeNP"))
        }