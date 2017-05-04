import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import axios from 'axios';

const style = {border: '1px solid black'};

class Message extends Component {
  constructor(props) {
    super(props);

    this.message = props.message;
  }

  render() {
    const msg = this.message;

    return (
      <tbody>
        <tr key={msg.id}>
          <td style={style}>{msg.message}</td>
          <td style={style}>{msg.percent}</td>
          <td style={style}><text>{msg.exception}</text></td>
          <td style={style}>{msg.created}</td>
        </tr>
      </tbody>
    )
  }
}

class Status extends Component {
  constructor(props) {
    super(props);

    this.state = {
      show: false,
    }

    this.status = props.status;
  }

  render() {
    const message_data = this.status.messages.map((message) => {
      return <Message key={message.id} message={message} />
    });

    const status = this.status;

    return (
      <tbody>
        <tr>
          <td style={style}>{status.status}</td>
          <td style={style}>{status.created}</td>
          <td style={style}><button onClick={(e) => { this.setState({show: !this.state.show}); }}>Show Messages</button></td>
        </tr>
        {this.state.show && (
          <tr>
            <td colSpan="3">
              <table style={{width: '100%'}}>
                {message_data}
              </table>
            </td>
          </tr>
        )}
      </tbody>
    )
  }
}

class Job extends Component {
  constructor(props) {
    super(props);

    this.state = {
      show: false,
    }

    this.job = props.job;
  }

  render() {
    const status_data = this.job.status.map((status) => {
      return <Status key={status.id} status={status} />
    });

    return (
      <tbody>
        <tr>
          <td style={style}>{this.job.server}</td>
          <td style={style}><button onClick={(e) => { this.setState({show: !this.state.show}); }}>Show Status</button></td>
        </tr>
        {this.state.show && (
          <tr>
            <td colSpan="2" style={style}>
              <table style={{width: '100%'}}>
                {status_data}
              </table>
            </td>
          </tr>
        )}
      </tbody>
    )
  }
}

class User extends Component {
  constructor(props) {
    super(props);

    this.state = {
      user: null,
      jobs: null,
      show: false
    }

    this.handleShowJobs = this.handleShowJobs.bind(this);
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

  handleShowJobs(event) {
    if (! this.state.jobs) {
      const jobsLocation = location.origin + '/wps/jobs/' + this.state.user.id;

      axios.get(jobsLocation)
        .then(res => {
          this.setState({jobs: res.data});
        })
        .catch(err => {
          console.log(err);
        });
    }

    this.setState({show: !this.state.show});
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

    let job_data = null;

    if (this.state.jobs) {
      const jobs = this.state.jobs;

      const job_rows = jobs.jobs.map((job) => {
        return <Job key={job.id} job={job} />
      });

      job_data = (
        <table style={{border: '1px solid black', width: '100%'}}>
          {job_rows} 
        </table>
      )
    }

    return (
      <div>
        <h1>User</h1>
        {user_data}
        <button onClick={this.handleShowJobs}>Show Jobs</button>
        {this.state.show && 
          job_data
        }
      </div>
    )
  }
}

export default User
