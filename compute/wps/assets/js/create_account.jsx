import React, { Component } from 'react';

import querystring from 'querystring';
import axios from 'axios';

class CreateAccount extends Component {
  constructor(props) {
    super(props);

    this.state = {
      username: '',
      email: '',
      openid: '',
      password: '',
      status: '',
    };

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
        window.location = location.origin + '/wps/debug/user';
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
        <form onSubmit={this.handleSubmit}>
          <label>
            Username:
            <input name="username" type="text" value={this.state.username} onChange={this.handleChange} />
          </label>
          <label>
            Email:
            <input name="email" type="text" value={this.state.email} onChange={this.handleChange} />
          </label>
          <label>
            OpenID:
            <input name="openid" type="text" value={this.state.openid} onChange={this.handleChange} />
          </label>
          <label>
            Password:
            <input name="password" type="password" value={this.state.password} onChange={this.handleChange} />
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

export default CreateAccount
