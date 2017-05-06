import React, { Component } from 'react';
import { withRouter } from 'react-router';
import { 
  Route,
  Link,
  Redirect,
  Switch,
} from 'react-router-dom';

import axios from 'axios';

import LoginComponent from './login.jsx';
import CreateAccount from './create_account.jsx';
import User from './user.jsx';

import Servers from './servers.jsx';
import Processes from './processes.jsx';
import Jobs from './jobs.jsx';

import AppBar from 'material-ui/AppBar';
import Drawer from 'material-ui/Drawer';
import FlatButton from 'material-ui/FlatButton';
import MenuItem from 'material-ui/MenuItem';

import injectTapEventPlugin from 'react-tap-event-plugin';

injectTapEventPlugin();

const PrivateRoute = ({ component: Component, isLogged, ...rest }) => {
  return <Route {...rest} render={props => (
    isLogged() ? 
    <Component {...props} /> :
    <Redirect to={{
      pathname: '/wps/debug/login',
      state: { from: props.location }
    }} />
  )} />
}

const Logged = (props) => (
  <FlatButton {...props} label="Logout" />
);

Logged.muiName = 'FlatButton';

const Login = (props) => (
  <FlatButton {...props} label="Login" />
);

Login.muiName = 'FlatButton';

class App extends Component {
  constructor(props) {
    super(props);

    this.state = {
      open: false,
      logged: localStorage.getItem('logged') || false
    }

    this.history = props.history;
  }

  handleLogin(e) {
    this.setState({ logged: true });

    localStorage.setItem('logged', this.state.logged)

    this.history.push('/wps/debug/user');
  }

  handleLogout(e) {
    const logoutURL = location.origin + '/auth/logout';

    axios.get(logoutURL)
      .then(res => {
        this.setState({ logged: false });

        localStorage.setItem('logged', this.state.logged)
      })
      .catch(err => {
        console.log(err);
      });
  }

  render() {
    return (
      <div>
        <AppBar
          title='LLNL WPS'
          onLeftIconButtonTouchTap={(e) => this.setState({ open: !this.state.open })}
          onTitleTouchTap={(e) => this.history.push('/wps/debug')}
          iconElementRight={this.state.logged ? 
              <Logged onTouchTap={(e) => this.handleLogout(e)} /> : 
              <Login onTouchTap={() => this.history.push('/wps/debug/login')} />}
        />
        <Drawer
          docked={false}
          open={this.state.open}
          onRequestChange={(open) => this.setState({open})}
        >
          <MenuItem primaryText="Servers" onTouchTap={(e) => this.history.push('/wps/debug/servers')} />
          {this.state.logged &&
            <MenuItem primaryText="Profile" onTouchTap={(e) => this.history.push('/wps/debug/user')} />
          }
        </Drawer>
        <div>
          <Switch>
            <Route exact path='/wps/debug' component={Home} />
            <Route path='/wps/debug/create' component={CreateAccount} />
            <PrivateRoute exact path='/wps/debug/user' isLogged={() => this.state.logged} component={User} />
            <Route exact path='/wps/debug/login' component={() => <LoginComponent handleLogin={(e) => this.handleLogin(e)} />} />
            <Route exact path='/wps/debug/servers' component={Servers} />
            <Route path='/wps/debug/servers/:server_id' component={Processes} />
            <PrivateRoute path='/wps/debug/user/:user_id/jobs' isLogged={() => this.state.logged} component={Jobs} />
            <Route component={NotFound} />
          </Switch>
        </div>
      </div>
    )
  }
}

const Home = () => <h1>Hello from Home!</h1>
const NotFound = () => <h1>404 Page is not found!</h1>

export default withRouter(App)
