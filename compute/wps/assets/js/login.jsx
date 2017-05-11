import React, { Component } from 'react';

import { Link } from 'react-router-dom';

import querystring from 'querystring';
import axios from 'axios';

import Card from 'material-ui/Card';
import TextField from 'material-ui/TextField';
import RaisedButton from 'material-ui/RaisedButton';

class Login extends Component {
  constructor(props) {
    super(props);

    this.state = {
      username: '',
      usernameError: '',
      password: '',
      passwordError: '',
    }

    this.handleLogin = props.handleLogin

    this.handleChange = this.handleChange.bind(this);
    this.handleSubmit = this.handleSubmit.bind(this);
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
    const postLocation = location.origin + '/auth/login/';

    const csrfToken = this.getCookie('csrftoken');

    axios.post(postLocation, querystring.stringify({
      username: [this.state.username],
      password: [this.state.password],
    }), {
      headers: {
        'X-CSRFToken': csrfToken,
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    })
      .then(res => {
        if (res.data.status == 'failure') {
          let newState = {};

          if (typeof(res.data.errors) == 'string') {
            newState.usernameError = ''

            newState.passwordError = res.data.errors;
          } else {
            if ('username' in res.data.errors) {
              newState.usernameError = res.data.errors['username'][0]; 
            }

            if ('password' in res.data.errors) {
              newState.passwordError = res.data.errors['password'][0];
            }
          }

          this.setState(newState);
        } else {
          this.handleLogin();
        }
      })
      .catch(err => {
        console.log(err);
      });

    event.preventDefault();
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
            onChange={this.handleChange}
            hintText="Username"
            errorText={this.state.usernameError}
            style={style.field}
          />
        </div>
        <div>
          <TextField
            type="password"
            name="password"
            value={this.state.password}
            onChange={this.handleChange}
            hintText="Password"
            errorText={this.state.passwordError}
            style={style.field}
          />
        </div>
        <div>
          <RaisedButton
            primary={true}
            type="submit"
            label="Submit"
            onTouchTap={e => this.handleSubmit(e)}
            style={style.field}
          />
        </div>
        <div>
          <Link
            style={style.field}
            to="/wps/debug/create"
          >
            Create an account
          </Link>
        </div>
      </Card>
    )
  }
}

export default Login
