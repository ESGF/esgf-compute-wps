var CDS = {};

CDS.WPSProcess = OpenLayers.Class({
    client: null,
    server: null,
    identifier: null,
    description: null,
    localWPS: "http://geoserver/wps",
    formats: null,
    chained: 0,
    executeCallbacks: null,
    initialize: function(a) {
        OpenLayers.Util.extend(this, a);
        this.executeCallbacks = [];
        this.formats = {
            "application/wkt": new OpenLayers.Format.WKT,
            "application/json": new OpenLayers.Format.GeoJSON
        }
    },
    describe: function(a) {
        a = a || {};
        if (!this.description) this.client.describeProcess(this.server, this.identifier, function(b) {
            this.description || this.parseDescription(b);
            a.callback && a.callback.call(a.scope, this.description)
        }, this);
        else if (a.callback) {
            var b = this.description;
            window.setTimeout(function() {
                a.callback.call(a.scope, b)
            }, 0)
        }
    },
    configure: function(a) {
        this.describe({
            callback: function() {
                var b = this.description,
                    c = a.inputs,
                    d, e, f;
                e = 0;
                for (f = b.dataInputs.length; e < f; ++e) d = b.dataInputs[e], this.setInputData(d, c[d.identifier]);
                a.callback && a.callback.call(a.scope)
            },
            scope: this
        });
        return this
    },
    execute: function(a) {
        this.configure({
            inputs: a.inputs,
            callback: function() {
                var b = this,
                    c = this.getOutputIndex(b.description.processOutputs, a.output);
                b.setResponseForm({
                    outputIndex: c
                });
                (function e() {
                    OpenLayers.Util.removeItem(b.executeCallbacks, e);
                    0 !== b.chained ? b.executeCallbacks.push(e) : OpenLayers.Request.POST({
                        url: b.client.servers[b.server].url,
                        data: (new OpenLayers.Format.WPSExecute).write(b.description),
                        success: function(e) {
                            result_obj = JSON.parse( e.responseText );
                            a.success && (g = {}, g[a.output || "result"] = e, a.success.call( a.scope, result_obj ) )
                        },
                        scope: b
                    })
                })()
            },
            scope: this
        })
    },
    output: function(a) {
        return new CDS.WPSProcess.ChainLink({
            process: this,
            output: a
        })
    },
    parseDescription: function(a) {
        a = this.client.servers[this.server];
        this.description = (new OpenLayers.Format.WPSDescribeProcess).read(a.processDescription[this.identifier]).processDescriptions[this.identifier]
    },
    setInputData: function(a, b) {
        delete a.data;
        delete a.reference;
        if (b instanceof CDS.WPSProcess.ChainLink) ++this.chained,
            a.reference = {
                method: "POST",
                href: b.process.server === this.server ? this.localWPS : this.client.servers[b.process.server].url
            }, b.process.describe({
                callback: function() {
                    --this.chained;
                    this.chainProcess(a, b)
                },
                scope: this
            });
        else {
            a.data = {};
            var c = a.complexData;
            c ? (c = this.findMimeType(c.supported.formats), a.data.complexData = {
                mimeType: c,
                value: this.formats[c].write(this.toFeatures(b))
            }) : a.data.literalData = {
                value: b
            }
        }
    },
    
    getMimeType: function(b) { 
    	mimeType = "undefined"
        if( b.hasOwnProperty('complexOutput') ) {
            mimeType = this.findMimeType(b.complexOutput.supported.formats, a.supportedFormats);
        }
        if( b.hasOwnProperty('literalOutput') ) {
            mimeType = this.findMimeType( b.literalOutput.dataType );
        }
        return mimeType 
    },  	
    	
    	
    setResponseForm: function(a) {
        a = a || {};
        var b = this.description.processOutputs[a.outputIndex || 0];

        this.description.responseForm = {
            rawDataOutput: {
                identifier: b.identifier,
                mimeType: this.getMimeType(b)
            }
        }
    },
    
    getOutputIndex: function(a, b) {
        var c;
        if (b)
            for (var d = a.length - 1; 0 <= d; --d) {
                if (a[d].identifier === b) {
                    c = d;
                    break
                }
            } else c = 0;
        return c
    },
    chainProcess: function(a, b) {
        var c = this.getOutputIndex(b.process.description.processOutputs, b.output);
        a.reference.mimeType = this.findMimeType(a.complexData.supported.formats, b.process.description.processOutputs[c].complexOutput.supported.formats);
        var d = {};
        d[a.reference.mimeType] = !0;
        b.process.setResponseForm({
            outputIndex: c,
            supportedFormats: d
        });
        for (a.reference.body = b.process.description; 0 < this.executeCallbacks.length;) this.executeCallbacks[0]()
    },
    toFeatures: function(a) {
        var b = OpenLayers.Util.isArray(a);
        b || (a = [a]);
        for (var c = Array(a.length), d, e = 0, f = a.length; e < f; ++e) d = a[e], c[e] = d instanceof OpenLayers.Feature.Vector ? d : new OpenLayers.Feature.Vector(d);
        return b ? c : c[0]
    },
    findMimeType: function(a, b) {
        b = b || this.formats;
        for (var c in a)
            if (c in b) return c
    },
    CLASS_NAME: "CDS.WPSProcess"
});
CDS.WPSProcess.ChainLink = OpenLayers.Class({
    process: null,
    output: null,
    initialize: function(a) {
        OpenLayers.Util.extend(this, a)
    },
    CLASS_NAME: "CDS.WPSProcess.ChainLink"
});
CDS.WPSClient = OpenLayers.Class({
    servers: null,
    version: "1.0.0",
    lazy: !1,
    events: null,
    initialize: function(a) {
        OpenLayers.Util.extend(this, a);
        this.events = new OpenLayers.Events(this);
        this.servers = {};
        for (var b in a.servers) this.servers[b] = "string" == typeof a.servers[b] ? {
            url: a.servers[b],
            version: this.version,
            processDescription: {}
        } : a.servers[b]
    },
    execute: function(a) {
        this.getProcess(a.server, a.process).execute({
            inputs: a.inputs,
            success: a.success,
            scope: a.scope
        })
    },
    getProcess: function(a, b) {
        var c = new CDS.WPSProcess({
            client: this,
            server: a,
            identifier: b
        });
        this.lazy || c.describe();
        return c
    },
    describeProcess: function(a, b, c, d) {
        var e = this.servers[a];
        e.processDescription[b] ? window.setTimeout(function() {
            c.call(d, e.processDescription[b])
        }, 0) : b in e.processDescription ? this.events.register("describeprocess", this, function g(a) {
            a.identifier === b && (this.events.unregister("describeprocess", this, g), c.call(d, a.raw))
        }) : (e.processDescription[b] = null, OpenLayers.Request.GET({
            url: e.url,
            params: {
                SERVICE: "WPS",
                VERSION: e.version,
                REQUEST: "DescribeProcess",
                IDENTIFIER: b
            },
            success: function(a) {
                e.processDescription[b] = a.responseText;
                this.events.triggerEvent("describeprocess", {
                    identifier: b,
                    raw: a.responseText
                })
            },
            scope: this
        }))
    },
    destroy: function() {
        this.events.destroy();
        this.servers = this.events = null
    },
    CLASS_NAME: "CDS.WPSClient"
});