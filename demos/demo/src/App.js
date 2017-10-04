import React, { Component } from 'react';
import NykaaAutoComplete from './NykaaAutoComplete';
import injectTapEventPlugin from 'react-tap-event-plugin';
import MuiThemeProvider from 'material-ui/styles/MuiThemeProvider';

// Needed for onTouchTap
// http://stackoverflow.com/a/34015469/988941
injectTapEventPlugin();

class App extends Component {
  render() {
    return (
      <MuiThemeProvider>
        <NykaaAutoComplete />
      </MuiThemeProvider>
    );
  }
}

export default App;
