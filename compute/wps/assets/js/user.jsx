import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import axios from 'axios';

import Card from 'material-ui/Card';
import RaisedButton from 'material-ui/RaisedButton';
import TextField from 'material-ui/TextField';
import { List, ListItem } from 'material-ui/List';
import {
  Table,
  TableBody,
  TableRow,
  TableRowColumn
} from 'material-ui/Table';

import LoginMPC from './login_mpc.jsx';
import LoginOAuth2 from './login_oauth2.jsx';

class User extends Component {
  constructor(props) {
    super(props);

    this.state = {
      user: null,
    }

    this.history = props.history;
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

  render() {
    let user_data = null;

    const style = {border: '1px solid black'};

    if (this.state.user) {
      const user = this.state.user;

      user_data = (
        <form>
          <List>
            <ListItem>
              <TextField name="username" value={user.username} readOnly="true" />
            </ListItem>
            <ListItem>
              <TextField name="email" value={user.email} readOnly="true" />
            </ListItem>
            <ListItem>
              <TextField name="type" value={user.type} readOnly="true" />
            </ListItem>
            <ListItem>
              <TextField name="api_key" value={user.api_key} readOnly="true" />
            </ListItem>
          </List>
        </form>
      )
    }

    return (
      <div>
        <Card>
          <Table>
            <TableBody displayRowCheckbox={false}>
              <TableRow>
                <TableRowColumn>
                  {user_data}
                </TableRowColumn>
              </TableRow>
            </TableBody>
          </Table>
        </Card>
        <Card>
          <LoginOAuth2 />
        </Card>
        <Card>
          <LoginMPC />
        </Card>
      </div>
    )
  }
}

export default User
