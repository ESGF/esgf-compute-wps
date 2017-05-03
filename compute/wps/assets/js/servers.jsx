import React, { Component } from 'react';

import axios from 'axios';

class Servers extends Component {
  constructor(props) {
    super(props);

    this.state = {
      servers: null,
    }

    this.handleShowGetCapabilities = this.handleShowGetCapabilities.bind(this);
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

  handleShowGetCapabilities(event) {
    const target = event.target;
    const key = target.name;

    let servers = this.state.servers;

    servers[key].show = !servers[key].show;

    this.setState({ servers: servers });
  }


  render() {
    const servers = this.state.servers;
    const style = { border: '1px solid black' };

    let data = null;

    if (this.state.servers) {
      data = Object.keys(servers).map((key) => {
        let server = servers[key];

        return (
          <tr key={key}>
            <td>
              <table>
                <tbody>
                  <tr key={key+'_1'}>
                    <td style={{border: '1px solid black', width: '100%'}}>{server.host}</td>
                    <td style={style}>{server.added}</td>
                    <td style={style}>{server.status}</td>
                    <td style={style}>
                      <button name={key} onClick={this.handleShowGetCapabilities}>GetCapabilities</button>
                    </td>
                  </tr>
                  <tr key={key+'_2'}>
                    <td>
                      {server.show ? server.capabilities : '' }
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
        <div>
          <h1>Servers</h1>
        </div>
        <table style={{border: '1px solid black', width: '100%'}}>
          <tbody>
            {data}
          </tbody>
        </table>
      </div>
    )
  }
}

export default Servers
