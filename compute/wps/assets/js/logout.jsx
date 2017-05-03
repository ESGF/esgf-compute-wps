import React, { Component } from 'react';

import axios from 'axios';

class Logout extends Component {
  constructor(props) {
    super(props);

    this.handleSubmit = this.handleSubmit.bind(this);
  }

  handleSubmit(event) {
    const postLocation = location.origin + '/auth/logout';

    axios.get(postLocation)
      .then(res => {
        console.log(res);
      })
      .catch(err => {
        console.log(err);
      });

    event.preventDefault();
  }

  render() {
    return (
      <form onSubmit={this.handleSubmit}>
        <input type="submit" value="Submit" />
      </form>
    )
  }
}

export default Logout
