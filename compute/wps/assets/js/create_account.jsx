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
      email: '',
      openid: '',
      password: '',
      status: '',
    };
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
  }

  render() {
    return (
      <Card>
        <div>
          <TextField
            name="username"
            value={this.state.username}
            onChange={e => this.handleChange(e)}
            hintText="Username"
          />
        </div>
        <div>
          <TextField
            name="email"
            value={this.state.email}
            onChange={e => this.handleChange(e)}
            hintText="Email"
          />
        </div>
        <div>
          <TextField
            name="openid"
            value={this.state.openid}
            onChange={e => this.handleChange(e)}
            hintText="OpenID"
          />
        </div>
        <div>
          <TextField
            name="password"
            value={this.state.password}
            onChange={e => this.handleChange(e)}
            hintText="Password"
          />
        </div>
        <div>
          <RaisedButton
            primary={true}
            label="Create"
            onTouchTap={e => this.handleSubmit(e)}
          />
        </div>
      </Card>
    )
  }
}

export default CreateAccount
