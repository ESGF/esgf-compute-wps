import React, { Component } from 'react';

export default class Jobs extends Component {
  constructor (props) {
    super(props);

    this.state = { jobs: [] };
  }

  componentDidMount () {
    $.ajax({
      url: 'http://0.0.0.0:8000/wps/api/jobs',
      dataType: 'json',
      success: (data) => {
        this.setState({ jobs: data });
      }
    });
  }

  render () {
    const { jobs } = this.state;
  
    const data = jobs.map((job) => {
      const history = job.history.map((item) => {
        return (
          <tr key={ item.pk }>
            <td>{ item.created_date }</td>
            <td>{ item.status }</td>
            <td>{ item.result }</td>
          </tr>
        );
      });

      return (
        <li key={ job.pk }>
          <div className="collapsible-header">{ job.server }</div>
          <div className="collapsible-body">
            <table className="bordered stripped">
              <tbody>
                { history }
              </tbody>
            </table>
          </div>
        </li>
      );
    });

    return (
      <div>
        <h1 className="center-align">Jobs</h1>
        <ul className="collapsible" data-collapsible="accordion">
          { data }
        </ul>
      </div>
    );
  }
}
