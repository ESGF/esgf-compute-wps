import React, { Component } from 'react';

import axios from 'axios';

import {
  Table,
  TableBody,
  TableRow,
  TableRowColumn
} from 'material-ui/Table';

import TextField from 'material-ui/TextField';
import RaisedButton from 'material-ui/RaisedButton';

const style = {border: '1px solid black'};

class Message extends Component {
  constructor(props) {
    super(props);

    this.message = props.message;
  }

  render() {
    const msg = this.message;

    return (
      <TableBody displayRowCheckbox={false}>
        <TableRow>
          <TableRowColumn>{msg.message}</TableRowColumn>
          <TableRowColumn>{msg.percent}</TableRowColumn>
          <TableRowColumn>{msg.exception}</TableRowColumn>
          <TableRowColumn>{msg.created}</TableRowColumn>
        </TableRow>
      </TableBody>
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
      <TableBody displayRowCheckbox={false}>
        <TableRow>
          <TableRowColumn>{status.status}</TableRowColumn>
          <TableRowColumn>{status.created}</TableRowColumn>
          <TableRowColumn><RaisedButton label="Show Messages" onTouchTap={(e) => { this.setState({show: !this.state.show})}}/></TableRowColumn>
        </TableRow>
        {this.state.show && (
          <TableRow>
            <TableRowColumn>
              <Table>
                {message_data}
              </Table>
            </TableRowColumn>
          </TableRow>
        )}
      </TableBody>
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
      <TableBody displayRowCheckbox={false}>
        <TableRow>
          <TableRowColumn>{this.job.server}</TableRowColumn>
          <TableRowColumn><RaisedButton label="Show Status" onTouchTap={(e) => { this.setState({show: !this.state.show})}} /></TableRowColumn>
        </TableRow>
        {this.state.show && (
          <TableRow>
            <TableRowColumn>
              <Table>
                {status_data}
              </Table>
            </TableRowColumn>
          </TableRow>
        )}
      </TableBody>
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
        <h1 style={{textAlign: 'center'}}>Jobs</h1>
        <Table>
          {this.state.jobs &&
            this.state.jobs.jobs.map((job) => {
              return <Job key={job.id} job={job} />
            })
          }
        </Table>
      </div>
    )
  }
}

export default Jobs
