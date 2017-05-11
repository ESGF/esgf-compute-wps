import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import querystring from 'querystring';
import axios from 'axios';

import {
  Table,
  TableBody,
  TableRow,
  TableRowColumn
} from 'material-ui/Table';

import Card from 'material-ui/Card';
import Dialog from 'material-ui/Dialog';
import FlatButton from 'material-ui/FlatButton';
import RaisedButton from 'material-ui/RaisedButton';
import TextField from 'material-ui/TextField';

class User extends Component {
  constructor(props) {
    super(props);

    this.state = {
      user: null,
      auth: 'mpc',
      open: false,
      username: '',
      usernameError: '',
      password: '',
      passwordError: '',
      openid: '',
      openidError: '',
    }

    this.history = props.history;
  }

  componentDidMount() {
    const userLocation = location.origin + '/auth/user';

    axios.get(userLocation)
      .then(res => {
        this.setState({
	  user: res.data,
	  username: res.data.username,
	  openid: res.data.openid,
	});
      })
      .catch(err => {
        console.log(err);
      });
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

  handleLogin(e) {
    let postURL = '';
    let data = null;

    if (this.state.auth == 'mpc') {
      postURL = location.origin + '/auth/login/mpc/';

      data = {
        openid: [this.state.openid],
        username: [this.state.username],
        password: [this.state.password],
      };
    } else {
      postURL = location.origin + '/auth/login/oauth2/';

      data = { openid: [this.state.openid] }
    }

    const csrfToken = this.getCookie('csrftoken');

    axios.post(postURL, querystring.stringify(data), {
        headers: {
          'X-CSRFToken': csrfToken,
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      })
      .then(res => {
	console.log(res.data);
        if (res.data.status === 'success') {
          this.setState({open: false});

	  if ('redirect' in res.data) {
	    window.location = res.data.redirect;
	  } else {
            this.forceUpdate();
	  }
        } else{
          if (typeof(res.data.errors) == 'string') {
            let newState = {
              usernameError: '',
              passwordError: res.data.errors,
              openidError: '',
            };

            this.setState(newState);
          } else {
            let newState = {};

            if ('username' in res.data.errors) {
              newState.usernameError = res.data.errors['username'][0];
            }

            if ('password' in res.data.errors) {
              newState.passwordError = res.data.errors['password'][0];
            }

            if ('openid' in res.data.errors) {
              newState.openidError = res.data.errors['openid'][0];
            }

            this.setState(newState);
          }
        }
      })
      .catch(err => {
        console.log(err);
      });
  }

  handleRegenerate(e) {
    const regenURL = location.origin + '/auth/user/' + this.state.user.id + '/regenerate';

    axios.get(regenURL)
      .then(res => {
        let user = this.state.user;

        user.api_key = res.data.api_key;

        this.setState({user: user});
      })
      .catch(err => {
        console.log(err);
      });
  }

  handleChange(e) {
    const target = e.target;
    const name = target.name;
    const value = target.value;

    this.setState({[name]: value});
  }

  render() {
    const style = {
      column: {
        width: '50%',
      },
      field: {
        width: '100%',
      },
    };

    const actions = [
      <FlatButton
        label="Submit"
        primary={true}
        onTouchTap={e => this.handleLogin(e)}
      />,
      <FlatButton
        label="Cancel"
        primary={true}
        onTouchTap={e => this.setState({open: false})}
      />
    ];

    return (
      <Card>
        <Table selectable={false}>
          <TableBody displayRowCheckbox={false}>
            <TableRow>
              <TableRowColumn style={style.column}>
                {this.state.user && (
                  <div>
                    <TextField
                      style={style.field}
                      name="username"
                      value={this.state.user.username}
                      readOnly="true" />
                    <br />
                    <TextField
                      style={style.field}
                      name="email"
                      value={this.state.user.email}
                      readOnly="true" />
                    <br />
                    <TextField
                      style={style.field}
                      name="openid"
                      value={this.state.user.openid}
                      readOnly="true"
                    />
                    <br />
                    <TextField
                      style={style.field}
                      name="type"
                      value={this.state.user.type}
                      readOnly="true" />
                    <br />
                    <TextField
                      style={style.field}
                      name="api_key"
                      value={this.state.user.api_key}
                      readOnly="true" />
                  </div>
                )}
              </TableRowColumn>
              <TableRowColumn>
                <RaisedButton
                  primary={true}
                  label="MyProxyClient"
                  onTouchTap={e => this.setState({open: true, auth: 'mpc'})}
                />
                <br />
                <br />
                <RaisedButton
                  primary={true}
                  label="OAuth2"
                  onTouchTap={e => this.setState({open: true, auth: 'oauth2' })}
                />
                <br />
                <br />
                <RaisedButton
                  primary={true}
                  label="Regenerate"
                  onTouchTap={e => this.handleRegenerate(e)}
                />
              </TableRowColumn>
            </TableRow>
          </TableBody>
        </Table>
        <Dialog
          actions={actions}
          model={true}
          open={this.state.open}
          onRequestClose={e => this.setState({open: false})}
        >
          <TextField
            name="openid"
            hintText="OpenID"
            errorText={this.state.openidError}
            value={this.state.openid}
            onChange={e => this.handleChange(e)}
          />
          <br />
          {this.state.auth == 'mpc' && 
              <TextField
                name="username"
                hintText="Username"
                errorText={this.state.usernameError}
                value={this.state.username}
                onChange={e => this.handleChange(e)}
              />
          }
          <br/>
          {this.state.auth == 'mpc' && 
              <TextField
                name="password"
                hintText="password"
                errorText={this.state.passwordError}
                value={this.state.password}
                type="password"
                onChange={e => this.handleChange(e)}
              />
          }
        </Dialog>
      </Card>
    )
  }
}

export default User
