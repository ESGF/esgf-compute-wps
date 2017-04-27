#! /usr/bin/env python

from django import forms

class OpenIDForm(forms.Form):
    openid = forms.CharField(label='OpenID URL', max_length=128)

class MPCForm(forms.Form):
    openid = forms.CharField(label='OpenID URL', max_length=128)
    username = forms.CharField(label='Username', max_length=128)
    password = forms.CharField(label='Password', max_length=128, widget=forms.PasswordInput)
