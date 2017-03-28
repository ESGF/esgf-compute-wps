import React, { Component } from 'react';

export default class Instances extends Component {
  constructor (props) {
    super(props);

    this.state  = { instances: [] };
  }

  componentDidMount () {
    $.ajax({
      url: 'http://0.0.0.0:8000/wps/instances',
      dataType: 'json',
      success: (data) => {
        this.setState({ instances: data });        
      }
    });
  }
    
  render () {
    const { instances } = this.state;

    const rows = instances.map((instance) => {
      let f = instance.fields;

      return (
        <tr>
          <td>{f.host}</td>
          <td>{f.added_date}</td>
          <td>{f.checked_date}</td>
          <td>{f.queue}</td>
          <td>{f.queue_size}</td>
          <td>{f.status}</td>
        </tr>
      );
    });

    return (
      <div>
        <h1 className="center-align">Instances</h1>
        <table className="bordered stripped">
          <thead>
            <tr>
              <td>Host</td>
              <td>Added</td>
              <td>Checked</td>
              <td>Queue</td>
              <td>Queue Size</td>
              <td>Status</td>
            </tr>
          </thead>
          <tbody>
            { rows }
          </tbody>
        </table>
      </div>
    );
  }
}
