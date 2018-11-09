from rest_framework.permissions import BasePermission


class IsRequestsToThemeAllow(BasePermission):
    def has_permission(self, request, view):
        return request.user.has_perm('global_permissions.theme')


class IsRequestsToAggregatorAllow(BasePermission):

    def has_permission(self, request, view):
        aggregator = request.query_params.get("aggregator", None)
        access = False

        if aggregator == "key_word" or "key_word" in request.query_params:
            access = request.user.has_perm('global_permissions.theme')
        elif aggregator == "publication" or "publication" in request.query_params:
            access = request.user.has_perm('global_permissions.publication')

        return access


class IsRequestsToPublicationAllow(BasePermission):
    def has_permission(self, request, view):
        return request.user.has_perm('global_permissions.publication')


class IsRequestsToSocialDemoAllow(BasePermission):
    def has_permission(self, request, view):
        return request.user.has_perm('global_permissions.social_demo')



