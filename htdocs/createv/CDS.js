var CDS = {
	title: "NCCS Climate Data Services",
	location: "NASA Goddard Space Flight Center",
	url: "https://cds.nccs.nasa.gov/"
};

CDS.wps = {
	title: "Web Processing Service Utilities",
	
	execute: function( args ) {
		var inputs = this.to_html_inputs( args.inputs ); 
		console.log( 'inputs: ' + inputs );
		var request = OpenLayers.Request.GET( {
     		url: args.url,
        	params: {
	           service: "WPS",
	           version: "1.0.0",
	           request: "Execute",
	           rawDataOutput: "result",
	           identifier: args.process,
	           datainputs: inputs
	        },
	        success: args.success
    	} );
    	console.log( request );
    },
    
    to_html_inputs: function( object ) {
    	var result = "[";
    	for (var property in object) {
    		if (object.hasOwnProperty(property)) {
				result = result.concat( property, "=", object[property], ";" );
    		}
		}
		result.concat( "]" );
		return result;
    }	
};
