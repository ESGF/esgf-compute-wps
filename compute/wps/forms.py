#! /usr/bin/env python

from django import forms

class UpdateForm(forms.Form):
    email = forms.EmailField(label='Email', required=False)

class OpenIDForm(forms.Form):
    openid_url = forms.URLField(label='OpenID URL', max_length=128)
    next = forms.CharField(label='Next')

class MPCForm(forms.Form):
    username = forms.CharField(label='Username', max_length=128)
    password = forms.CharField(label='Password', max_length=128, widget=forms.PasswordInput)
