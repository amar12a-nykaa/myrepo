import React, { Component } from 'react';
import Autosuggest from 'react-autosuggest';
import $ from 'jquery';
import './App.css';

class NykaaAutoComplete extends Component {
  constructor (props) {
    super(props);
    
    this.state = {
      value: '',
      suggestions: []
    };
  }

  onChange = (event, { newValue }) => {
    this.setState({
      value: newValue
    });
  };

  filterSuggestions (searchText, key) {
    console.log(searchText);
    console.log(key);
  }

  fetchSuggestions = ({ value }) => {
    if (value.length) {
      var url = 'http://52.77.36.71/apis/v1/search.suggestions?q=' + value;
      $.ajax({
        url: url, 
        dataType: 'json',
        cache: false,
      }).then((res) => this.handleSuccess(res),
              (err) => this.handleFailure(err));
    } else {
      this.setState({
        suggestions: []
      });
    }
  };

  clearSuggestions = () => {
    this.setState({
      suggestions: []
    });
  };

  getSuggestionValue = suggestion => suggestion.q;

  renderSuggestion(suggestion) {
    return (
      <span>
        {suggestion.q}
        {suggestion.category && <span className="cat-section">in {suggestion.category}</span>}
      </span>
    );
  }

  onSuggestionSelected = (event, { suggestion, suggestionValue, suggestionIndex, sectionIndex, method }) => {
    var win = window.open(suggestion.url, '_blank');
    win.focus();
  };

  handleSuccess (response) {
    var suggestions = [];
    for(var i = 0; i < response['suggestions'].length; i++) {
      var suggestion = response['suggestions'][i];
      if (suggestion['type'] === 'brand' && typeof suggestion['categories'] !== 'undefined') {
        for (var j = 0; j < suggestion['categories'].length; j++) {
          var brand_suggestion = suggestion['categories'][j];
          brand_suggestion['q'] = suggestion['q'];
          suggestions.push(brand_suggestion);
        }
      } else {
        suggestions.push(suggestion);
      }
    }
    this.setState({suggestions: suggestions});
  }

  handleFailure (err) {
    console.log(err);
  }

  render() {
    const { value, suggestions } = this.state;

    // Autosuggest will pass through all these props to the input.
    const inputProps = {
      placeholder: 'Search any product..',
      value,
      onChange: this.onChange
    };

    return (
      <div className='App'>
        <Autosuggest
          suggestions={suggestions}
          onSuggestionsFetchRequested={this.fetchSuggestions}
          onSuggestionsClearRequested={this.clearSuggestions}
          getSuggestionValue={this.getSuggestionValue}
          renderSuggestion={this.renderSuggestion}
          onSuggestionSelected={this.onSuggestionSelected}
          inputProps={inputProps}
        />
      </div>
    );
  }
}

export default NykaaAutoComplete;
