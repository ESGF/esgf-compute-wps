import React, { Component } from 'react';
import { 
  BrowserRouter as Router,
  Route,
  Switch,
} from 'react-router-dom';

class App extends Component {
  render() {
    return (
      <Router>
        <Switch>
          <Route path='/' component={Home} />
        </Switch>
      </Router>
    )
  }
}

const Home = () => <h1>Hello from Home!</h1>

export default App
