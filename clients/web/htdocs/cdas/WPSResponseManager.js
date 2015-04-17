var wpsResponseManager = {
	
	process: function( responseStr, embedded, plotter ) {
		var timerId = undefined;
		var pingInterval = 300;
        var debug = false;
		
	/*	var isNumeric = function(val) {
		    return Number(parseFloat(val))==val;
	};  
	*/		
		function extractHREF(val) {
		    var index = val.indexOf("href='");
		    if (index == -1) { return undefined; }
		    var subval = val.substr( index+6 );
		    index = subval.indexOf("'");
		    if (index == -1) { return undefined; }
		    return "http://" + subval.substr( 0, index );
		};
		
		var HttpClient = function() {
		    this.get = function(aUrl, aCallback) {
		        var anHttpRequest = new XMLHttpRequest();
		        anHttpRequest.onreadystatechange = function() { 
		            if (anHttpRequest.readyState == 4 && anHttpRequest.status == 200)
		                aCallback(anHttpRequest.responseText);
		        };
		        anHttpRequest.open( "GET", aUrl, true );            
		        anHttpRequest.send( null );
		    };
		};
		
		function plotResponseJson( response_json ) {
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
                plotter(JSON.parse(response_json));
            }
		};
	
		function processPlotRequest( response ) {
			var  lines= response.split("\n");
			var response_json = '';
			for( var i = 0; i<lines.length; i++ ){
				var line = lines[i];
                if ( ( line.length > 0 ) &&  ( line.indexOf("Content-Type:") == -1 ) ) {
                    response_json = (response_json.length == 0) ? line : response_json + line;
                };
			};
			if ( response_json.length > 0 ) { 
				if( timerId != undefined) { clearInterval( timerId ); }
				plotResponseJson( response_json );
			};
		};
		
		
		if ( embedded ) {
			processPlotRequest( responseStr );
		} else {
			var href = extractHREF( responseStr );
			var fetcher = new HttpClient();
			timerId = setInterval( function() { fetcher.get( href, processPlotRequest ); }, pingInterval );
		};	
	
	},
};
	
