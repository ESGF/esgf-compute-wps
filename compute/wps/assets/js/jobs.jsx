import React, { Component } from 'react';

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
          <td style={{border: '1px solid black', width: '80%'}}>
            <textarea style={{width: '100%'}} readOnly="true" rows="10" value={msg.exception ? msg.exception : ''} />
          </td>
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

class Jobs extends Component {
  constructor(props) {
    super(props);

    this.state = {
      jobs: null
    };

    this.user_id = props.match.params.user_id;
  }

  componentDidMount() {
    const jobsURL = location.origin + '/wps/jobs/' + this.user_id;

    axios.get(jobsURL)
      .then(res => {
        this.setState({ jobs: res.data });
      })
      .catch(err => {
        console.log(err);
      });
  }

  render() {
    return (
      <div>
        <h1>Jobs</h1>
        <table>
          {this.state.jobs &&
            this.state.jobs.jobs.map((job) => {
              return <Job key={job.id} job={job} />
            })
          }
        </table>
      </div>
    )
  }
}

export default Jobs
