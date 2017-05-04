import React, { Component } from 'react';

import axios from 'axios';

class Processes extends Component {
  constructor(props) {
    super(props);

    this.server_id = props.match.params.server_id;

    this.state = {
      processes: null
    }

    this.handleShowDescribeProcess = this.handleShowDescribeProcess.bind(this);
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

  handleShowDescribeProcess(event) {
    const target = event.target;
    const name = target.name;

    let processes = this.state.processes;

    processes[name].show = !processes[name].show;

    this.setState({processes: processes});
  }

  render() {
    let data = null;

    const style = {border: '1px solid black'};

    if (this.state.processes) {
      data = Object.keys(this.state.processes).map((key) => {
        const process = this.state.processes[key]; 

        return (
          <tr key={key}>
            <td>
              <table>
                <tbody>
                  <tr key={key+'_1'}>
                    <td style={{border: '1px solid black', width: '100%'}}>{process.identifier}</td>
                    <td style={style}>{process.backend}</td>
                    <td style={style}>
                      <button name={key} onClick={this.handleShowDescribeProcess}>DescribeProcess</button>
                    </td>
                  </tr>
                  <tr key={key+'_2'}>
                    <td>
                      {process.show ? process.description : ''}
                    </td>
                  </tr>
                </tbody>
              </table>
            </td>
          </tr>
        )
      });
    }

    return (
      <div>
        <h1>Processes</h1>
        <table style={{border: '1px solid black', width: '100%'}}>
          <tbody>
            {data}
          </tbody>
        </table>
      </div>
    )
  }
}

export default Processes
