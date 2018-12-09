from rest_framework.pagination import PageNumberPagination


class ConfiguredPageNumberPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'per_page'
    max_page_size = 1000