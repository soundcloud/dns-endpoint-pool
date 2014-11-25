var EndpointPool,
    _      = require('underscore'),
    dns    = require('dns'),
    Events = require('events'),
    util   = require('util'),

    CLOSED = 0,
    HALF_OPEN_READY = 1,
    HALF_OPEN_PENDING = 2,
    OPEN = 3;

module.exports = EndpointPool = function (discoveryName, ttl, maxFailures, resetTimeout) {
  if (!discoveryName || !ttl || !maxFailures || !resetTimeout) {
    throw new Error('Must supply all arguments');
  }
  Events.EventEmitter.call(this);

  this.discoveryName = discoveryName;
  this.ttl = ttl;
  this.endpoints = [];
  this._endpointOffset = 0;
  this._updateTimeout = null;
  this.maxFailures = maxFailures;
  this.resetTimeout = resetTimeout;
  this.update();
};

util.inherits(EndpointPool, Events.EventEmitter);

_.extend(EndpointPool.prototype, {
  update: function () {
    this.resolve(function (err, endpoints) {
      if (err) {
        this.emit('error', err);
      } else {
        // endpoints = [/*endpoints[0],*/ {name: 'localhost', port: 1337 }];
        this.setEndpoints(endpoints);
      }
      this._updateTimeout = setTimeout(this.update.bind(this), this.ttl);
    }.bind(this));
  },

  resolve: function (cb) {
    dns.resolveSrv(this.discoveryName, cb);
  },

  getEndpoint: function () {
    var endpoint, i, l, offset;
    for (i = 0, l = this.endpoints.length; i < l; ++i) {
      offset = (this._endpointOffset + i) % l;
      endpoint = this.endpoints[offset];

      switch (endpoint.state) {
        case HALF_OPEN_READY:
          endpoint.state = HALF_OPEN_PENDING; // let one through, then turn it off again
          /* falls through */
        case CLOSED:
          this._endpointOffset = offset + 1;
          return endpoint;
        // case OPEN: case HALF_OPEN_PENDING: // continue to the next one
      }
    }
    this.emit('noEndpoints');
    return null;
  },

  setEndpoints: function (endpoints) {
    var newEndpoints, i, matchingEndpoint;

    newEndpoints = endpoints.map(function (info) {
      return new Endpoint(info, this.maxFailures, this.resetTimeout);
    }, this);

    for (i = this.endpoints.length; i--;) {
      matchingEndpoint = _.findWhere(newEndpoints, { url: this.endpoints[i].url });

      if (matchingEndpoint) { // found a match, remove it from `newEndpoints`, since it's not new
        newEndpoints = _.without(newEndpoints, matchingEndpoint);
      } else { // didn't find a match in endpoints, so kill that endpoint
        this.endpoints.splice(i, 1);
      }
    }
    // push all the actually-new endpoints in
    this.endpoints.push.apply(this.endpoints, newEndpoints);
  },

  stopUpdating: function () {
    clearTimeout(this._updateTimeout);
  }
});

function Endpoint(info, maxFailures, resetTimeout) {
  this.name = info.name;
  this.port = info.port;
  this.url = info.name + ':' + info.port;
  this.state = CLOSED;
  this.failCount = 0;
  this.callback = endpointCallback.bind(this);
  this.disable = disableEndpoint.bind(this);
  this.resetTimeout = resetTimeout;
  this.maxFailures = maxFailures;
}

function endpointCallback(err) {
  if (err) {
    if (this.state === OPEN) {
      return;
    }
    // failed while half open
    if (this.state === HALF_OPEN_PENDING) {
      this.disable();
      return;
    }

    // first failure, set up a window
    if (this.failCount === 0) {
      this._timeout = setTimeout(function () {
        this.failCount = 0;
      }.bind(this), this.resetTimeout);
    }

    this.failCount++;

    if (this.failCount >= this.maxFailures) {
      this.disable();
    }
  } else {
    clearInterval(this._timeout);
    clearInterval(this._reopenTimeout);
    this.failCount = 0;
    this.state = CLOSED;
  }
}

function disableEndpoint() {
  this.state = OPEN;
  clearInterval(this._timeout);
  clearInterval(this._reopenTimeout);
  this._reopenTimeout = setTimeout(function () {
    this.state = HALF_OPEN_READY;
  }.bind(this), this.resetTimeout);
}
