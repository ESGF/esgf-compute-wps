import React, { Component } from 'react';
import { Link } from 'react-router-dom';

import axios from 'axios';

class User extends Component {
  constructor(props) {
    super(props);

    this.state = {
      user: null,
      jobs: null
    }
  }

  componentDidMount() {
    const userLocation = location.origin + '/auth/user';

    axios.get(userLocation)
      .then(res => {
        this.setState({user: res.data});

        const jobsLocation = location.origin + '/wps/jobs/' + res.data.id;

        axios.get(jobsLocation)
          .then(res => {
            this.setState({jobs: res.data});
          })
          .catch(err => {
            console.log(err);
          });
      })
      .catch(err => {
        console.log(err);
      });
  }

  render() {
    let data = null;
    let job_data = null;

    const style = {border: '1px solid black'};

    if (this.state.jobs) {
      const jobs = this.state.jobs;

      job_data = Object.keys(jobs).map((key) => {
        const job = jobs[key];

        let status_data = Object.keys(job.status).map((key1) => {
          const status = job.status[key1];

          let message_data = Object.keys(status.messages).map((key3) => {
            const message = status.messages[key3];

            return (
              <tr key={key+'_'+key1+'_'+key3}>
                <td>{message.message}</td>
                <td>{message.percent}</td>
                <td>{message.exception}</td>
              </tr>
            )
          });

          return (
            <tbody>
              <tr key={key+'_'+key1}>
                <td style={style}>{status.status}</td>
                <td style={style}>{status.created}</td>
              </tr>
              <tr key={key+'_'+key1+'_1'}>
                <td style={style}>
                  <table>
                    <tbody>
                      {message_data}
                    </tbody>
                  </table>
                </td>
              </tr>
            </tbody>
          )
        });

        return (
          <tr key={key}>
            <td>
              <table style={style}>
                {status_data}
              </table>
            </td>
          </tr>
        )
      });
    }

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
        <br/>
        <table style={style}>
          <tbody>
            {job_data}
          </tbody>
        </table>
      </div>
    )
  }
}

export default User
