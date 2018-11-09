from noksfishes.models import Publication, Theme


def run():
    for title in Publication.objects.all().values_list("key_word", flat=True).distinct():
        Theme.objects.get_or_create(title=title)
