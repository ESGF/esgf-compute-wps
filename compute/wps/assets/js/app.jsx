import React, { Component } from 'react';
import { 
  BrowserRouter as Router,
  Route,
  Switch,
} from 'react-router-dom';

import Login from './login.jsx';
import Logout from './logout.jsx';
import LoginMPC from './login_mpc.jsx';
import CreateAccount from './create_account.jsx';
import Servers from './servers.jsx';
import Processes from './processes.jsx';
import User from './user.jsx';

class App extends Component {
  render() {
    return (
      <Router>
        <Switch>
          <Route exact path='/wps/debug/' component={Home} />
          <Route path='/wps/debug/create/' component={CreateAccount} />
          <Route path='/wps/debug/user/' component={User} />
          <Route path='/wps/debug/login/' component={Login} />
          <Route path='/wps/debug/logout/' component={Logout} />
          <Route path='/wps/debug/login/mpc/' component={LoginMPC} />
          <Route exact path='/wps/debug/servers/' component={Servers} />
          <Route path='/wps/debug/servers/:server_id' component={Processes} />
          <Route component={NotFound} />
        </Switch>
      </Router>
    )
  }
}

const Home = () => <h1>Hello from Home!</h1>
const NotFound = () => <h1>404 Page is not found!</h1>

export default App
