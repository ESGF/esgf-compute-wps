import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import axios from 'axios';

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

  handleShowJobs(e) {
    this.history.push('/wps/debug/user/' + this.state.user.id + '/jobs');
  }

  render() {
    let user_data = null;

    const style = {border: '1px solid black'};

    if (this.state.user) {
      const user = this.state.user;

      user_data = (
        <table style={{border: '1px solid black', width: '100%'}}>
          <tbody>
            <tr>
              <td style={style}>{user.username}</td>
              <td style={style}>{user.email}</td>
              <td style={style}>{user.type}</td>
              <td style={style}>{user.api_key}</td>
            </tr>
          </tbody>
        </table>
      )
    }

    return (
      <div>
        <h1>User</h1>
        <button onClick={(e) => this.handleShowJobs(e)}>Jobs</button>
        <br />
        {user_data}
        <div>
          <div>
            <LoginOAuth2 />
          </div>
          <br />
          <div>
            <LoginMPC />
          </div>
        </div>
      </div>
    )
  }
}

export default User
