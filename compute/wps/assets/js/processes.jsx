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

class Process extends Component {
  constructor(props) {
    super(props);

    this.process = props.process;
    this.handleShowProcess = props.handleShowProcess;
  }

  render() {
    return (
      <Table>
        <TableBody displayRowCheckbox={false}>
          <TableRow>
            <TableRowColumn>{this.process.identifier}</TableRowColumn>
            <TableRowColumn>{this.process.backend}</TableRowColumn>
            <TableRowColumn>
              <RaisedButton
                label="Describe"
                primary={true}
                onTouchTap={e => this.handleShowProcess(this.process.description)}
              />
            </TableRowColumn>
          </TableRow>
        </TableBody>
      </Table>
    )
  }
}

class Processes extends Component {
  constructor(props) {
    super(props);

    this.state = {
      processes: null,
      open: false,
      process: null,
    }

    this.server_id = props.match.params.server_id;
  }

  componentDidMount() {
    const processesLocation = location.origin + '/wps/servers/' + this.server_id;

    axios.get(processesLocation)
      .then(res => {
        this.setState({ processes: res.data });
      })
      .catch(err => {
        console.log(err);
      });
  }

  render() {
    return (
      <div>
        <h1 style={{textAlign: 'center'}}>Processes</h1>
        <Table>
          <TableBody displayRowCheckbox={false}>
            {this.state.processes && (
              Object.keys(this.state.processes).map(key => {
                return (
                  <TableRow key={key}>
                    <TableRowColumn>
                      <Process
                        process={this.state.processes[key]}
                        handleShowProcess={proc => this.setState({open: true, process: proc})}
                      />
                    </TableRowColumn>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
        <Dialog
          title="Description"
          modal={false}
          open={this.state.open}
          onRequestClose={e => this.setState({open: false})}
        >
          <TextField
            id="description"
            value={this.state.process}
            fullWidth={true}
            multiLine={true}
            rowsMax={16}
          />
        </Dialog>
      </div>
    )
  }
}

export default Processes
