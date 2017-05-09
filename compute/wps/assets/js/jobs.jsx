import React, { Component } from 'react';

import axios from 'axios';

import {
  Table,
  TableBody,
  TableRow,
  TableRowColumn
} from 'material-ui/Table';

import Dialog from 'material-ui/Dialog';
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
      <Table selectable={false}>
        <TableBody displayRowCheckbox={false}>
          <TableRow displayBorder={false}>
            <TableRowColumn>{msg.message || 'No Message'}</TableRowColumn>
            <TableRowColumn>{msg.percent || '0'}</TableRowColumn>
            <TableRowColumn>{msg.created}</TableRowColumn>
          </TableRow>
        </TableBody>
      </Table>
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
    this.onShowDialog = props.onShowDialog;
  }

  render() {
    return (
      <Table selectable={false}>
        <TableBody displayRowCheckbox={false}>
          <TableRow displayBorder={this.state.show}>
            <TableRowColumn>{this.status.status}</TableRowColumn>
            <TableRowColumn>{this.status.created}</TableRowColumn>
            <TableRowColumn>
              {this.status.status == 'ProcessStarted' && 
                <RaisedButton 
                  primary={true}
                  label="Messages"
                  onTouchTap={e => { this.setState({show: !this.state.show})}}
                />
              }
              {this.status.status == 'ProcessFailed' &&
                <RaisedButton
                  primary={true}
                  label="Exception"
                  onTouchTap={e => this.onShowDialog(this.status.messages[0].exception)}
                />
              }
            </TableRowColumn>
          </TableRow>
          {this.state.show && (
            this.status.messages.map(message => {
              return (
                <TableRow key={message.id}>
                  <TableRowColumn colSpan="3" style={{paddingLeft: '0px', paddingRight: '0px'}}>
                    <Message message={message} />
                  </TableRowColumn>
                </TableRow>
              )
            })
          )}
        </TableBody>
      </Table>
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
    this.onShowDialog = props.onShowDialog;
  }

  render() {
    return (
      <Table selectable={false}>
        <TableBody displayRowCheckbox={false}>
          <TableRow displayBorder={this.state.show}>
            <TableRowColumn>{this.job.server}</TableRowColumn>
            <TableRowColumn>
              <RaisedButton
                style={{marginLeft: 'auto', marginRight: '0px'}}
                primary={true}
                label="status"
                onTouchTap={(e) => this.setState({ show: !this.state.show })}
              />
            </TableRowColumn>
          </TableRow>
          {this.state.show && (
            this.job.status.map(status => {
              return (
                <TableRow key={status.id}>
                  <TableRowColumn colSpan="2" style={{paddingLeft: '0px', paddingRight: '0px'}}>
                    <Status
                      status={status}
                      onShowDialog={this.onShowDialog}
                    />
                  </TableRowColumn>
                </TableRow>
              )
            })
          )}
        </TableBody>
      </Table>
    )
  }
}

class Jobs extends Component {
  constructor(props) {
    super(props);

    this.state = {
      jobs: null,
      open: false,
      dialogText: '',
    };

    this.user_id = props.match.params.user_id;
  }

  componentDidMount() {
    const jobsURL = location.origin + '/wps/jobs/' + this.user_id;

    axios.get(jobsURL)
      .then(res => {
        this.setState({ jobs: res.data.jobs });
      })
      .catch(err => {
        console.log(err);
      });
  }

  render() {
    return (
      <div>
        <h1 style={{textAlign: 'center'}}>Jobs</h1>
        <Table selectable={false}>
          <TableBody displayRowCheckbox={false}>
            {this.state.jobs &&
              this.state.jobs.map(job => {
                return (
                  <TableRow key={job.id}>
                    <TableRowColumn>
                      <Job
                        job={job}
                        onShowDialog={text => this.setState({open: true, dialogText: text})}
                      />
                    </TableRowColumn>
                  </TableRow>
                )
              })
            }
          </TableBody>
        </Table>
        <Dialog
          modal={false}
          open={this.state.open}
          onRequestClose={e => this.setState({open: false})}
        >
          <TextField
            name='exception'
            fullWidth={true}
            multiLine={true}
            value={this.state.dialogText}
          />
        </Dialog>
      </div>
    )
  }
}

export default Jobs
