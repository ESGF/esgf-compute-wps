#! /usr/bin/env python

from django import forms

class OpenIDForm(forms.Form):
    openid = forms.CharField(label='OpenID URL', max_length=128)
