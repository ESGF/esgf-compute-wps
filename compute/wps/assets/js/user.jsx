import React, { Component } from 'react';

import axios from 'axios';

class User extends Component {
  constructor(props) {
    super(props);

    this.state = {
      user: null
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

  render() {
    let data = null;

    const style = {border: '1px solid black'};

    if (this.state.user) {
      const user = this.state.user;

      data = (
        <tr key="1">
          <td style={style}>{user.username}</td>
          <td style={style}>{user.email}</td>
          <td style={style}>{user.type}</td>
          <td style={style}>{user.api_key}</td>
        </tr>
      )
    }

    return (
      <div>
        <h1>User</h1>
        <table style={style}>
          <tbody>
            {data}
          </tbody>
        </table>
      </div>
    )
  }
}

export default User
