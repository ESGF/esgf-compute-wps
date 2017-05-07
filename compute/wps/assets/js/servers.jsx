import React, { Component } from 'react';
import { Link } from 'react-router-dom';

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

class Server extends Component {
  constructor(props) {
    super(props);

    this.state = {
      show: false,
    }

    this.handleShowCapabilities = props.handleShowCapabilities;
    this.server = props.server;
    this.history = props.history;
    this.id = props.id;
  }

  render() {
    return (
      <Table>
        <TableBody displayRowCheckbox={false}>
          <TableRow>
            <TableRowColumn>{this.server.host}</TableRowColumn>
            <TableRowColumn>{this.server.added}</TableRowColumn>
            <TableRowColumn>{this.server.status}</TableRowColumn>
            <TableRowColumn>
              <RaisedButton
                label="Capabilities"
                primary={true}
                onTouchTap={e => this.handleShowCapabilities(this.server.capabilities)}
              />
            </TableRowColumn>
            <TableRowColumn>
              <RaisedButton
                label="Processes"
                primary={true}
                onTouchTap={e => this.history.push('/wps/debug/servers/' + this.id)}
              />
            </TableRowColumn>
          </TableRow>
        </TableBody>
      </Table>
    )
  }
}

class Servers extends Component {
  constructor(props) {
    super(props);

    this.state = {
      servers: null,
      open: false,
      capabilities: null,
    }

    this.history = props.history;
  }

  componentDidMount() {
    const serverLocation = location.origin + '/wps/servers';

    axios.get(serverLocation)
      .then(res => {
        this.setState({ servers: res.data });
      })
      .catch(err => {
        console.log(err);
      });
  }

  render() {
    return (
      <div>
        <h1 style={{textAlign: 'center'}}>Servers</h1>
        <Table>
          <TableBody displayRowCheckbox={false}>
            {this.state.servers && 
                Object.keys(this.state.servers).map(key => {
                  return (
                    <TableRow key={key}>
                      <TableRowColumn>
                        <Server
                          id={key}
                          history={this.history}
                          server={this.state.servers[key]}
                          handleShowCapabilities={cap => this.setState({open: true, capabilities: cap})}
                        />
                      </TableRowColumn>
                    </TableRow>
                  )
                })
            }
          </TableBody>
        </Table>
        <Dialog
          title="Capabilities"
          modal={false}
          open={this.state.open}
          onRequestClose={e => this.setState({open: false})}
        >
          <TextField id="capabilities" fullWidth={true} multiLine={true} value={this.state.capabilities} />
        </Dialog>
      </div>
    )
  }
}

export default Servers
