import React from 'react';
import ReactDOM from 'react-dom';

import { Router, Route } from 'react-router';
import { HashRouter, Link } from 'react-router-dom';

import Servers from './Servers.jsx';
import Instances from './Instances.jsx';
import Processes from './Processes.jsx';
import Jobs from './Jobs.jsx';

ReactDOM.render(
  <HashRouter>
    <div>
      <nav>
        <div className="nav-wrapper">
          <Link to="/" className="brand-logo right">WPS Debugger</Link>
          <ul id="nav-mobile" className="left">
            <li><Link to="/servers">Servers</Link></li>
            <li><Link to="/instances">Instances</Link></li>
            <li><Link to="/processes">Processes</Link></li>
            <li><Link to="/jobs">Jobs</Link></li>
          </ul>
        </div>
      </nav>
      <div className="row">
        <div className="col s12">
          <div className="card">
            <div className="card-content">
              <Route path="/servers" component={Servers}/>
              <Route path="/instances" component={Instances}/>
              <Route path="/processes" component={Processes}/>
              <Route path="/jobs" component={Jobs}/>
            </div>
          </div>
        </div>
      </div>
    </div>
  </HashRouter>,
  document.getElementById('container')
);
