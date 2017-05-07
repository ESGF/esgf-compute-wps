import React, { Component } from 'react';
import { Link } from 'react-router-dom';

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
      password: '',
      openid: '',
    }
  }

  componentDidMount() {
    const userLocation = location.origin + '/auth/user';

    axios.get(userLocation)
      .then(res => {
        this.setState({user: res.data});
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
    if (this.state.auth == 'mpc') {
      const postURL = location.origin + '/auth/login/mpc/';

      const data = {
        openid: [this.state.openid],
        username: [this.state.username],
        password: [this.state.password],
      };
    } else {
      const postURL = location.origin + '/auth/login/oauth2/';

      const data = { openid: [this.state.openid] }
    }

    const csrfToken = this.getCookie('csrftoken');

    axios.post(postURL, querystring.stringify(data), {
        headers: {
          'X-CSRFToken': csrfToken,
          'Content-Type': 'application/x-www-form-urlencoded'
        }
      })
      .then(res => {
        if (res.data.status === 'success') {
          window.location = location.origin + '/wps/debug/user';
        } else{
          this.setState({status: JSON.stringify(res.data.errors)});
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
      field: {
        width: '650px'
      }
    };

    const actions = [
      <FlatButton
        label="Submit"
        primary={true}
      />,
      <FlatButton
        label="Cancel"
        primary={true}
        onTouchTap={e => this.setState({open: false})}
      />
    ];

    return (
      <Card>
        <Table>
          <TableBody displayRowCheckbox={false}>
            <TableRow>
              <TableRowColumn>
                {this.state.user && (
                  <form>
                    <TextField
                      name="username"
                      value={this.state.user.username}
                      readOnly="true" />
                    <br />
                    <TextField
                      name="email"
                      value={this.state.user.email}
                      readOnly="true" />
                    <br />
                    <TextField
                      name="type"
                      value={this.state.user.type}
                      readOnly="true" />
                    <br />
                    <TextField
                      style={style.field}
                      name="api_key"
                      value={this.state.user.api_key}
                      readOnly="true" />
                  </form>
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
            value={this.state.openid}
          />
          <br />
          {this.state.auth == 'mpc' && 
              <TextField
                name="username"
                hintText="Username"
                value={this.state.username}
              />
          }
          <br/>
          {this.state.auth == 'mpc' && 
              <TextField
                name="password"
                hintText="password"
                value={this.state.password}
                type="password"
              />
          }
        </Dialog>
      </Card>
    )
  }
}

export default User
