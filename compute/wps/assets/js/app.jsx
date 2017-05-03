import React, { Component } from 'react';
import { Router, Route } from 'react-router';
import { HashRouter } from 'react-router-dom';

class App extends Component {
  render() {
    return (
      <HashRouter>
        <Route path='/' component={Home} />
      </HashRouter>
    )
  }
}

const Home = () => <h1>Hello from Home!</h1>

export default App
