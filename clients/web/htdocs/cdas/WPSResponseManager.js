var wpsResponseManager = {

    _timerId: undefined,
    _debug: false,
    _plotter: undefined,

    process: function( responseStr, embedded, plotter ) {
		var pingInterval = 500;
        this._plotter = plotter;

		if ( embedded ) {
			this._processPlotRequest( responseStr );
		} else {
			var href = this._extractHREF( responseStr );
			var fetcher = new this.HttpClient();
			this._timerId = setInterval( function() { fetcher.get( href, this._processPlotRequest ); }, pingInterval );
		};
	},

    HttpClient: function() {
        this.get = function(aUrl, aCallback) {
            var anHttpRequest = new XMLHttpRequest();
            anHttpRequest.onreadystatechange = function() {
                if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
                    aCallback(anHttpRequest.responseText);
            };
            anHttpRequest.open( "GET", aUrl, true );
            anHttpRequest.send( null );
        };
	},

    _extractHREF: function (val) {
        var index = val.indexOf("href='");
        if (index == -1) { return undefined; }
        var subval = val.substr( index+6 );
        index = subval.indexOf("'");
        if (index == -1) { return undefined; }
        return "http://" + subval.substr( 0, index );
	},

    _processPlotRequest: function ( response ) {
        var  lines= response.split("\n");
        var response_json = '';
        for( var i = 0; i<lines.length; i++ ){
            var line = lines[i];
            if ( ( line.length > 0 ) &&  ( line.indexOf("Content-Type:") == -1 ) ) {
                response_json = (response_json.length == 0) ? line : response_json + line;
            };
        };
        if ( response_json.length > 0 ) {
            if( this._timerId != undefined) { clearInterval( this._timerId ); }
            this._plotResponseJson( response_json );
        };
    },

    _plotResponseJson: function ( response_json ) {
        response_json = response_json.replace(/\bNaN\b/g, "null");
        if ( debug ) { console.log( "Got response: " + response_json ); }
        if( response_json.trim().substr(0,10).search('<?xml') >= 0 ) {
            var xml = jQuery.parseXML(response_json);
            var exceptions = xml.getElementsByTagName('Exception');
            for ( var iE=0; iE<exceptions.length; iE++ ) {
                var exception = exceptions[iE];
                var exception_code = exception.getAttribute("exceptionCode");
                var locator = exception.getAttribute("locator");
                var exception_text_elems = exception.getElementsByTagName("ExceptionText");
                var exception_text = ( exception_text_elems.length > 0 ) ? exception_text_elems[0].innerHTML.replace(/\\/g, '') : "";
                alert( "Server signaled an Exception:\n" + exception_code + ": " + locator + "\n\n" + exception_text );
            };
        }  else {
            this._plotter(JSON.parse(response_json));
        }
    }

};
	
