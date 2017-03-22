import React, { Component } from 'react';

export default class Debug extends Component {
  constructor (props) {
    super(props);

    this.state = { servers: [] };
  }

  componentDidMount () {
    $.ajax({
      url: 'http://0.0.0.0:8000/wps/servers',
      datatype: 'json',
      success: (data) => {
        this.setState({ servers: data });
      }
    });
  }

  render () {
    const { servers } = this.state;

    return (
      <div>
        <h1>WPS Debug</h1>
        <table className="bordered stripped">
          <thead>
            <tr>
              <td>Host</td>
              <td>Added</td>
              <td>Status</td>
            </tr>
          </thead>
          <tbody>
          {
            servers.map((server) => {
              return (
                <tr>
                  <td>{server.fields.host}</td>
                  <td>{server.fields.added_date}</td>
                  <td>{server.fields.status}</td>
                </tr>
              );
            })
          }
          </tbody>
        </table>
      </div>
    );
  }
}
