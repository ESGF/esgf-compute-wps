#! /usr/bin/env python

from django import forms

class OpenIDForm(forms.Form):
    openid = forms.CharField(label='OpenID URL', max_length=128)
    username = forms.CharField(label='Username', max_length=128)
    password = forms.CharField(label='Password', max_length=128, widget=forms.PasswordInput)
    service = forms.ChoiceField(label='Authentication Service', choices=(('myproxyclient', 'MyProxyClient'), ('oauth2', 'OAuth2')))
