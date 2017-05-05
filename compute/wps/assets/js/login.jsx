import React, { Component } from 'react';

import querystring from 'querystring';
import axios from 'axios';

class Login extends Component {
  constructor(props) {
    super(props);

    this.state = {
      username: '',
      password: ''
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
      this.handleLogin();
    })
    .catch(err => {
      console.log(err);
    });

    event.preventDefault();
  }

  render() {
    return (
      <form onSubmit={this.handleSubmit}>
        <label>
          Username:
          <input name="username" type="text" value={this.state.username} onChange={this.handleChange} />
        </label>
        <label>
          Password:
          <input name="password" type="password" value={this.state.password} onChange={this.handleChange} />
        </label>
        <input type="submit" value="Submit" />
      </form>
    )
  }
}

export default Login
