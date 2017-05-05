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
        <form>
          <label>
            Username:
            <input type="text" value={user.username} readOnly="true" />
          </label>
          <label>
            Email:
            <input type="text" value={user.email} readOnly="true" />
          </label>
          <label>
            Type:
            <input type="text" value={user.type} readOnly="true" />
          </label>
          <label>
            API Key:
            <input type="text" value={user.api_key} readOnly="trie" />
          </label>
        </form>
      )
    }

    return (
      <div>
        <h1>User</h1>
        <div>
          <button onClick={(e) => this.handleShowJobs(e)}>Jobs</button>
        </div>
        <br />
        <div>
          {user_data}
        </div>
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
