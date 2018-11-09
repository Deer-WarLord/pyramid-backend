# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from django import forms
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.forms import ReadOnlyPasswordHashField
from django.utils.translation import ugettext_lazy as _

from uploaders.models import *


class UserCreationForm(forms.ModelForm):

    provider = forms.ModelChoiceField(queryset=Provider.objects.all(), required=False)
    password1 = forms.CharField(label='Password', widget=forms.PasswordInput)
    password2 = forms.CharField(label='Password confirmation', widget=forms.PasswordInput)

    class Meta:
        model = User
        fields = ('username', 'provider',)

    def clean_password2(self):
        password1 = self.cleaned_data.get("password1")
        password2 = self.cleaned_data.get("password2")
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError("Passwords don't match")
        return password2

    def save(self, commit=True):
        user = super(UserCreationForm, self).save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()
        return user


class UserChangeForm(forms.ModelForm):

    provider = forms.ModelChoiceField(queryset=Provider.objects.all(), required=False)
    password = ReadOnlyPasswordHashField(
            label=_("Password"),
            help_text=_(
                    "Raw passwords are not stored, so there is no way to see this "
                    "user's password, but you can change the password using "
                    "<a href=\"../password/\">this form</a>."
            ),
    )

    class Meta:
        model = User
        fields = '__all__'

    def clean_password(self):
        return self.initial["password"]


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    form = UserChangeForm
    add_form = UserCreationForm
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'provider', 'get_groups')
    fieldsets = (
        (None, {'fields': ('username', 'password', 'provider')}),
        (_('Personal info'), {'fields': ('first_name', 'last_name', 'email')}),
        (_('Permissions'), {'fields': ('is_active', 'is_staff', 'is_superuser',
                                       'groups', 'user_permissions')}),
        (_('Important dates'), {'fields': ('last_login', 'date_joined')}),
    )
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('username', 'provider', 'password1', 'password2')}
         ),
    )

    def get_groups(self, obj):
        return "; ".join([g.name for g in obj.groups.all()])

@admin.register(Provider)
class ProviderAdmin(admin.ModelAdmin):
    list_display = ['title', 'description', 'parser_info']


@admin.register(UploadedInfo)
class UploadedInfoAdmin(admin.ModelAdmin):
    list_display = ['get_provider', 'title', 'file', 'created_date']
    list_filter = ['provider__title']

    def get_provider(self, obj):
        return "%s [%d]" % (obj.provider.title, obj.provider.id)
    # readonly_fields = ('created_date',)
    # fieldsets = (
    #     (None, {'fields': ('provider', 'title', 'created_date')}),
    # )
