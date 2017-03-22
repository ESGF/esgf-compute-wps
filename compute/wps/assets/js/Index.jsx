import React from 'react';
import ReactDOM from 'react-dom';

import { Router, Route } from 'react-router';
import { HashRouter, Link } from 'react-router-dom';

import Servers from './Servers.jsx';

ReactDOM.render(
  <HashRouter>
    <div>
      <nav>
        <div className="nav-wrapper">
          <Link to="/" className="brand-logo right">WPS Debugger</Link>
          <ul id="nav-mobile" className="left">
            <li><Link to="/servers">Servers</Link></li>
          </ul>
        </div>
      </nav>
      <div className="row">
        <div className="col s12">
          <div className="card">
            <div className="card-content">
              <Route path="/servers" component={Servers}/>
            </div>
          </div>
        </div>
      </div>
    </div>
  </HashRouter>,
  document.getElementById('container')
);
