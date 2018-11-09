# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from uploaders.models import UploadedInfo


class AnalyzedInfo(models.Model):
    upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE, editable=False, related_name="noksfishes",
                                    verbose_name='Ключ импорта')
    title = models.CharField(max_length=255, verbose_name='Тема/Компания')
    code = models.PositiveIntegerField(verbose_name='Код')
    url = models.URLField(max_length=255, verbose_name='Ссылка')
    site = models.CharField(max_length=255, verbose_name='Название')
    posted_date = models.DateTimeField(verbose_name='Дата публикации')
    created_date = models.DateTimeField(auto_now_add=True, verbose_name='Дата добавления')

    class Meta:
        verbose_name = "Analyzed Info"
        verbose_name_plural = "Analyzed Info"

    
class Category(models.Model):
    title = models.CharField(max_length=255, verbose_name='Категория')


class Theme(models.Model):
    title = models.CharField(max_length=255, verbose_name='Тема')


class Market(models.Model):
    name = models.CharField(max_length=255, verbose_name='Рынок')


class Publication(models.Model):

    upload_info = models.ForeignKey(UploadedInfo, on_delete=models.CASCADE, editable=False,
                                    related_name="noksfishes_publications",
                                    verbose_name='Ключ импорта',
                                    null=True, blank=True)

    market = models.ForeignKey(Market, on_delete=models.CASCADE,
                                    related_name="market_publications",
                                    verbose_name='ID Рынка',
                                    null=True, blank=True)

    key_word = models.CharField(max_length=255, verbose_name='Ключевое слово', null=True, blank=True)
    object = models.CharField(max_length=255, verbose_name='Объект', null=True, blank=True)
    title = models.TextField(verbose_name='Заголовок', null=True, blank=True)
    inserted_date = models.DateTimeField(auto_now_add=True, verbose_name='Дата постановки в базу')
    posted_date = models.DateField(verbose_name='Дата выхода', null=True, blank=True)
    posted_time = models.TimeField(verbose_name='Время выхода', null=True, blank=True)
    end_time = models.TimeField(verbose_name='Время окончания', null=True, blank=True)
    category = models.CharField(max_length=255, verbose_name='Категория', null=True, blank=True)
    url = models.URLField(max_length=1024, verbose_name='Ссылка', null=True, blank=True)
    priority = models.CharField(max_length=255, verbose_name='Важность', null=True, blank=True)
    advertisement = models.CharField(max_length=255, verbose_name='Реклама', null=True, blank=True)
    size = models.PositiveIntegerField(verbose_name='Размер', null=True, blank=True)
    symbols = models.PositiveIntegerField(verbose_name='Знаков', null=True, blank=True)
    publication = models.CharField(max_length=255, verbose_name='Издание', null=True, blank=True)
    source = models.TextField(verbose_name='Первоисточник', null=True, blank=True)
    country = models.CharField(max_length=255, verbose_name='Страна', null=True, blank=True)
    region = models.CharField(max_length=255, verbose_name='Регион', null=True, blank=True)
    city = models.CharField(max_length=255, verbose_name='Город', null=True, blank=True)
    regionality = models.CharField(max_length=255, verbose_name='Региональность', null=True, blank=True)
    type = models.CharField(max_length=255, verbose_name='Тип Издания', null=True, blank=True)
    topic = models.CharField(max_length=255, verbose_name='Вид Издания', null=True, blank=True)
    consolidated_type = models.CharField(max_length=255, verbose_name='Сводный тип', null=True, blank=True)
    number = models.PositiveIntegerField(verbose_name='Номер издания', null=True, blank=True)
    printing = models.PositiveIntegerField(verbose_name='Тираж', null=True, blank=True)
    page = models.CharField(max_length=255, verbose_name='Страница', null=True, blank=True)
    fill_rate = models.PositiveIntegerField(verbose_name='Рейтинг наполнения', null=True, blank=True)
    user = models.CharField(max_length=255, verbose_name='Пользователь', null=True, blank=True)
    author_tone = models.CharField(max_length=255, verbose_name='Тон автора', null=True, blank=True)
    event_tone = models.CharField(max_length=255, verbose_name='Тон события', null=True, blank=True)
    general_tone = models.CharField(max_length=255, verbose_name='Тон Общий', null=True, blank=True)
    objectivity = models.CharField(max_length=255, verbose_name='Объектность', null=True, blank=True)
    mention_type = models.CharField(max_length=255, verbose_name='Вид упоминания', null=True, blank=True)
    eventivity = models.CharField(max_length=255, verbose_name='Событийность', null=True, blank=True)
    subject = models.CharField(max_length=512, verbose_name='Тематики', null=True, blank=True)
    key_material = models.CharField(max_length=512, verbose_name='Ключевой материал', null=True, blank=True)
    plot = models.CharField(max_length=255, verbose_name='Сюжеты', null=True, blank=True)
    author = models.TextField(verbose_name='Автор', null=True, blank=True)
    top_managers = models.CharField(max_length=255, verbose_name='Top-менджеры', null=True, blank=True)
    companies = models.CharField(max_length=255, verbose_name='Компании', null=True, blank=True)
    heading = models.CharField(max_length=255, verbose_name='Рубрика', null=True, blank=True)
    annotation = models.TextField(verbose_name='Аннотация', null=True, blank=True)
    full_text = models.TextField(verbose_name='Полный текст', null=True, blank=True)
    comment = models.TextField(verbose_name='Комментарий', null=True, blank=True)
    subtitle = models.TextField(verbose_name='Подзаголовок', null=True, blank=True)
    reference = models.TextField(verbose_name='Справка', null=True, blank=True)
    w = models.FloatField(verbose_name='W', null=True, blank=True)
    y = models.FloatField(verbose_name='Y', null=True, blank=True)
    z = models.FloatField(verbose_name='Z', null=True, blank=True)
    fc = models.FloatField(verbose_name='ФС', null=True, blank=True)
    r = models.FloatField(verbose_name='R', null=True, blank=True)
    ce = models.FloatField(verbose_name='СЭ', null=True, blank=True)
    f = models.FloatField(verbose_name='F', null=True, blank=True)
    r_small = models.FloatField(verbose_name='r', null=True, blank=True)
    s = models.FloatField(verbose_name='S', null=True, blank=True)
    e = models.FloatField(verbose_name='E', null=True, blank=True)
    ta = models.FloatField(verbose_name='Ta', null=True, blank=True)
    ts = models.FloatField(verbose_name='Ts', null=True, blank=True)
    tc = models.FloatField(verbose_name='Tc', null=True, blank=True)
    tmk = models.FloatField(verbose_name='Tmk', null=True, blank=True)
    tmc = models.FloatField(verbose_name='Tmc', null=True, blank=True)
    l = models.FloatField(verbose_name='L', null=True, blank=True)
    d = models.FloatField(verbose_name='D', null=True, blank=True)
    ktl = models.FloatField(verbose_name='Ktl', null=True, blank=True)
    kt = models.FloatField(verbose_name='kt', null=True, blank=True)
    kl = models.FloatField(verbose_name='kl', null=True, blank=True)
    m = models.FloatField(verbose_name='M', null=True, blank=True)
    pg = models.FloatField(verbose_name='Pg', null=True, blank=True)
    h = models.FloatField(verbose_name='H', null=True, blank=True)
    result = models.FloatField(verbose_name='Результат', null=True, blank=True)
    periodicity = models.CharField(max_length=255, verbose_name='Периодичность', null=True, blank=True)
    edition_address = models.CharField(max_length=255, verbose_name='Адрес издания', null=True, blank=True)
    activity = models.FloatField(verbose_name='Активность издания', null=True, blank=True)
    marginality = models.FloatField(verbose_name='Маргинальность', null=True, blank=True)
    cost = models.FloatField(verbose_name='Стоимость', null=True, blank=True)
    visitors = models.FloatField(verbose_name='Посетителей', null=True, blank=True)
    citation_index = models.FloatField(verbose_name='Индекс цитирования', null=True, blank=True)
    width = models.FloatField(verbose_name='Ширина', null=True, blank=True)
    k1 = models.FloatField(verbose_name='k1', null=True, blank=True)
    k2 = models.FloatField(verbose_name='k2', null=True, blank=True)
    k3 = models.FloatField(verbose_name='k3', null=True, blank=True)
    dtek_kof = models.FloatField(verbose_name='ДТЭК коэффициент', null=True, blank=True)
    created_date = models.DateTimeField(verbose_name='Дата создания', null=True, blank=True)
    edit_date = models.DateTimeField(verbose_name='Дата изменения', null=True, blank=True)
    note = models.CharField(max_length=255, verbose_name='Пометка', null=True, blank=True)

    class Meta:
        verbose_name = "Publication"
        verbose_name_plural = "Publications"


class ShukachPublication(models.Model):

    publication = models.OneToOneField(Publication, on_delete=models.CASCADE, primary_key=True)
    shukach_id = models.IntegerField()





