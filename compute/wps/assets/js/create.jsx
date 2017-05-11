import React, { Component } from 'react';

import querystring from 'querystring';
import axios from 'axios';

import Card from 'material-ui/Card';
import TextField from 'material-ui/TextField';
import RaisedButton from 'material-ui/RaisedButton';

class CreateAccount extends Component {
  constructor(props) {
    super(props);

    this.state = {
      username: '',
      usernameError: '',
      email: '',
      emailError: '',
      openid: '',
      openidError: '',
      password: '',
      passwordError: '',
    };

    this.history = props.history;
  }

  getCookie(name) {
    let cookieValue = null;

    if (document.cookie && document.cookie != '') {
      const cookies = document.cookie.split(';');

      for (let i = 0; i < cookies.length; i++) {
        var cookie = jQuery.trim(cookies[i]);

        if (cookie.substring(0, name.length + 1) == (name + '=')) {
          cookieValue = decodeURIComponent(cookie.substring(name.length + 1));

          break;
        }
      }
    }

    return cookieValue;
  }

  handleChange(event) {
    const target = event.target;
    const name = target.name;
    const value = target.value;

    this.setState({ [name]: value });
  }

  handleSubmit(event) {
    const postLocation = location.origin + '/auth/create/';

    const csrfToken = this.getCookie('csrftoken');

    axios.post(postLocation, querystring.stringify({
      username: [this.state.username],
      email: [this.state.email],
      openid: [this.state.openid],
      password: [this.state.password],
    }), {
      headers: {
        'X-CSRFToken': csrfToken,
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    })
    .then(res => {
      if (res.data.status == 'success') {
        this.history.push('/wps/debug/user');
      } else {
        const errors = res.data.errors;

        let newState = {};

        if ('username' in errors) {
          newState.usernameError = errors['username'][0];
        }

        if ('password' in errors) {
          newState.passwordError = errors['password'][0];
        }

        if ('openid' in errors) {
          newState.openidError = errors['openid'][0];
        }

        if ('email' in errors) {
          newState.emailError = errors['email'][0];
        }

        this.setState(newState);
      }
    })
    .catch(err => {
      console.log(err);
    });
  }

  render() {
    const style = {
      field: {
        margin: '8px',
      },
    };

    return (
      <Card>
        <div>
          <TextField
            name="username"
            value={this.state.username}
            onChange={e => this.handleChange(e)}
            style={style.field}
            hintText="Username"
            errorText={this.state.usernameError}
          />
        </div>
        <div>
          <TextField
            name="email"
            value={this.state.email}
            onChange={e => this.handleChange(e)}
            style={style.field}
            hintText="Email"
            errorText={this.state.emailError}
          />
        </div>
        <div>
          <TextField
            name="openid"
            value={this.state.openid}
            onChange={e => this.handleChange(e)}
            style={style.field}
            hintText="OpenID"
            errorText={this.state.openidError}
          />
        </div>
        <div>
          <TextField
            name="password"
            value={this.state.password}
            onChange={e => this.handleChange(e)}
            style={style.field}
            hintText="Password"
            errorText={this.state.passwordError}
          />
        </div>
        <div>
          <RaisedButton
            primary={true}
            label="Create"
            onTouchTap={e => this.handleSubmit(e)}
            style={style.field}
          />
        </div>
      </Card>
    )
  }
}

export default CreateAccount
