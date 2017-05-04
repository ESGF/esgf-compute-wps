import React, { Component } from 'react';
import { Redirect } from 'react-router-dom';

import querystring from 'querystring';
import axios from 'axios';

class LoginOAuth2 extends Component {
  constructor(props) {
    super(props);

    this.state = {
      openid: '',
      status: null
    }

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

    this.setState({openid: target.value});
  }

  handleSubmit(event) {
    const oauth2Location = location.origin + '/auth/login/oauth2/';

    const csrfToken = this.getCookie('csrftoken');

    axios.post(oauth2Location, querystring.stringify({
        openid: [this.state.openid]
      }), {
        headers: {
          'X-CSRFToken': csrfToken,
          'Content-Type': 'application/x-www-form-urlencoded',
        }
      })
      .then(res => {
        if (res.data.status === 'success') {
          window.location = res.data.redirect;
        } else {
          this.setState({status: JSON.stringify(res.data.errors)});
        }
      })
      .catch(err => {
        console.log(err);
      });

    event.preventDefault();
  }

  render() {
    return (
      <div>
        <h1>Login OAuth2</h1>
        <form onSubmit={this.handleSubmit}>
          <label>
            OpenID:
            <input type="text" value={this.state.openid} onChange={this.handleChange} />
          </label>
          <input type="submit" value="Submit" />
        </form>
        <div>
          {this.state.status &&
              this.state.status
          }
        </div>
      </div>
    )
  }
}

export default LoginOAuth2
