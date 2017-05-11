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
import CreateAccount from './create.jsx';
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
      pathname: '/wps/home/login',
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
      user: null,
      logged: localStorage.getItem('logged') === 'true' || false
    }

    this.history = props.history;
  }

  componentDidMount() {
    const userURL = location.origin + '/auth/user';

    axios.get(userURL)
      .then(res => {
        if ('status' in res.data) {
          localStorage.setItem('logged', false);
        } else {
          this.setState({user: res.data, logged: true});

          localStorage.setItem('logged', true);
        }
      })
      .catch(err => {
        console.log(err);
      });
  }

  handleLogin(e) {
    this.setState({ logged: true });

    localStorage.setItem('logged', true);

    this.history.push('/wps/home/user');
  }

  handleLogout(e) {
    const logoutURL = location.origin + '/auth/logout';

    axios.get(logoutURL)
      .then(res => {
        this.setState({ logged: false });

        localStorage.setItem('logged', false);
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
          onTitleTouchTap={(e) => this.history.push('/wps/home')}
          iconElementRight={this.state.logged ? 
              <Logged onTouchTap={(e) => this.handleLogout(e)} /> : 
              <Login onTouchTap={() => this.history.push('/wps/home/login')} />}
        />
        <Drawer
          docked={false}
          open={this.state.open}
          onRequestChange={(open) => this.setState({open})}
        >
          <MenuItem primaryText="Servers" onTouchTap={(e) => this.history.push('/wps/home/servers')} />
          {this.state.logged && (
            <MenuItem primaryText="Profile" onTouchTap={(e) => this.history.push('/wps/home/user')} />
          )}
          {this.state.logged && (
            <MenuItem
              primaryText="Jobs"
              onTouchTap={(e) => this.history.push('/wps/home/user/' + this.state.user.id + '/jobs')}
            />
          )}
        </Drawer>
        <Switch>
          <Route exact path='/wps/home' component={Home} />
          <Route path='/wps/home/create' component={CreateAccount} />
          <PrivateRoute exact path='/wps/home/user' isLogged={() => this.state.logged} component={User} />
          <Route exact path='/wps/home/login' component={() => <LoginComponent handleLogin={(e) => this.handleLogin(e)} />} />
          <Route exact path='/wps/home/servers' component={Servers} />
          <Route path='/wps/home/servers/:server_id' component={Processes} />
          <PrivateRoute path='/wps/home/user/:user_id/jobs' isLogged={() => this.state.logged} component={Jobs} />
          <Route component={NotFound} />
        </Switch>
      </div>
    )
  }
}

const Home = () => (
<div>
  <p>
    Welcome to LLNL's WPS Server.
  </p>
  <p>
    To get started create an account. In the user page you can authenticate using
    OAuth2 or MyProxyClient. With your API_KEY you can now access the server.
  </p>
  <p>
    Use your new API_KEY with <a href="https://github.com/ESGF/esgf-compute-api">esgf-compute-api</a> to access
    the server.
  </p>
</div>
);

const NotFound = () => <h1>404 Page is not found!</h1>

export default withRouter(App)
