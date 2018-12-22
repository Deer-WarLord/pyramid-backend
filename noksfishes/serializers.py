from rest_framework import serializers
from noksfishes.models import *
from uploaders.utils import URLField, DateTimeField


class AnalyzedInfoSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalyzedInfo
        fields = '__all__'

    upload_info = serializers.ReadOnlyField()
    url = URLField(max_length=255)
    posted_date = DateTimeField()


class PublicationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publication
        fields = '__all__'

    upload_info = serializers.CharField(source='upload_info.title', required=False)
    url = URLField(max_length=1024, allow_blank=True)
    inserted_date = DateTimeField()
    created_date = DateTimeField(allow_null=True)
    edit_date = DateTimeField(allow_null=True)


class PublicationTitleDateSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=2048)
    posted_date = serializers.DateField()
    count = serializers.IntegerField()



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


class ExportTvPublicationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publication
        fields = ['id_article', 'id_title', 'key_word', 'title', 'posted_date', 'posted_time', 'end_time', 'publication']

    id_article = serializers.IntegerField(source='id')
    id_title = serializers.SerializerMethodField()

    def get_id_title(self, obj):
        return Theme.objects.get_or_create(title=obj.key_word)[0].id


class ShukachResponseSerializer(serializers.Serializer):

    data = serializers.DictField(child=serializers.CharField(), required=False)
    count = serializers.IntegerField(required=False)
    ajx_status = serializers.CharField()
    ajx_mess = serializers.CharField(allow_blank=True)

    def validate(self, obj):
        if obj["ajx_status"].lower() != "ok":
            raise serializers.ValidationError(obj["ajx_mess"])

        if len(obj['data']) != obj['count']:
            raise serializers.ValidationError("data length is not equal to count")
        return obj


class ShukachIdSerializer(serializers.ModelSerializer):
    class Meta:
        model = ShukachPublication
        fields = ['shukach_id']

    shukach_id = serializers.IntegerField()


class IdAdeptSerializer(serializers.ModelSerializer):
    class Meta:
        model = AdeptPublication
        fields = ['id_adept']

    id_adept = serializers.IntegerField()


class ShukachPublicationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Publication
        fields = '__all__'

    upload_info = serializers.CharField(source='upload_info.title', required=False)
    url = URLField(max_length=1024, allow_blank=True)
    posted_date = DateTimeField(allow_null=True)
    inserted_date = DateTimeField(allow_null=True)
    shukach_id = ShukachIdSerializer()
    id_adept = IdAdeptSerializer(required=False)

    def to_internal_value(self, data):

        text = data["full_text"]
        for i in range(1, 6):
            if "full_text_%d" % i in data:
                info = data.pop("full_text_%d" % i)
                if info:
                    text += info

        data["full_text"] = text

        if data["posted_date"] and " " in data["posted_date"]:
            data["posted_date"], data["posted_time"] = data["posted_date"].split()

        if data["market"]:
            market_obj = Market.objects.get_or_create(name=data.pop("market"))
            data["market"] = market_obj[0].id
        else:
            pass

        data["shukach_id"] = {"shukach_id": data["shukach_id"]}
        if "id_adept" in data:
            data["id_adept"] = {"id_adept": data["id_adept"]}

        return super(ShukachPublicationSerializer, self).to_internal_value(data=data)

    def create(self, validated_data):
        shukach_id = validated_data.pop('shukach_id')['shukach_id']
        id_adept = None
        if "id_adept" in validated_data:
            id_adept = validated_data.pop('id_adept')['id_adept']
        publication = Publication.objects.create(**validated_data)
        ShukachPublication.objects.create(publication=publication, shukach_id=shukach_id)
        if id_adept:
            AdeptPublication.objects.create(publication=publication, adept_id=id_adept)
        return publication