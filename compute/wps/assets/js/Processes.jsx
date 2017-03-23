import React, { Component } from 'react';

export default class Processes extends Component {
  constructor (props) {
    super(props);

    this.state = { processes: [] };
  }

  componentDidMount () {
    $.ajax({
      url: 'http://0.0.0.0:8000/wps/processes',
      dataType: 'json',
      success: (data) => {
        this.setState({ processes: data });
      }
    });
  }

  render () {
    const { processes } = this.state;

    const data = processes.map((process) => {
      let f = process.fields;

      return (
        <tr>
          <td>{f.identifier}</td>
          <td>{f.started}</td>
          <td>{f.completed}</td>
        </tr>
      );
    });

    return (
      <div>
        <h1 className="center-align">Processes</h1>
        <table className="bordered stripped">
          <thead>
            <tr>
              <td>Identifier</td>
              <td>Started</td>
              <td>Completed</td>
            </tr>
          </thead>
          <tbody>
            { data }
          </tbody>
        </table>
      </div>
    );
  }
}
