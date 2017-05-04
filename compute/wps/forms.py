#! /usr/bin/env python

from django import forms

class CreateForm(forms.Form):
    username = forms.CharField(label='Username', max_length=128)
    email = forms.EmailField(label='Email')
    password = forms.CharField(label='Password', max_length=128, widget=forms.PasswordInput)
    openid = forms.CharField(label='OpenID', max_length=128)

class LoginForm(forms.Form):
    username = forms.CharField(label='Username', max_length=128)
    password = forms.CharField(label='Password', max_length=128, widget=forms.PasswordInput)

class OpenIDForm(forms.Form):
    openid = forms.CharField(label='OpenID URL', max_length=128)

class MPCForm(forms.Form):
    openid = forms.CharField(label='OpenID URL', max_length=128)
    username = forms.CharField(label='Username', max_length=128)
    password = forms.CharField(label='Password', max_length=128, widget=forms.PasswordInput)
