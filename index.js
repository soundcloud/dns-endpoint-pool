var EndpointPool,
    _      = require('underscore'),
    dns    = require('dns'),
    Events = require('events'),
    util   = require('util'),

    DNS_LOOKUP_TIMEOUT = 1000,

    CLOSED            = 0,  // closed circuit: endpoint is good to use
    HALF_OPEN_READY   = 1,  // endpoint is in recovery state: offer it for use once
    HALF_OPEN_PENDING = 2,  // endpoint recovery is in process
    OPEN              = 3;  // open circuit: endpoint is no good

/**
 * @param {String} discoveryName The name of the service discovery host
 * @param {Number} ttl           How long the endpoints are valid for. The service discovery endpoint will be checked on
 *                               this interval.
 * @param {Number} maxFailures   Number of failures allowed before the endpoint circuit breaker is tripped.
 * @param {Number} failureWindow Size of the sliding window of time in which the failures are counted.
 * @param {Number} resetTimeout  Amount of time before putting the circuit back into half open state.
 */
module.exports = EndpointPool = function (discoveryName, ttl, maxFailures, failureWindow, resetTimeout) {
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
  this.failureWindow = failureWindow;
  this.resetTimeout = resetTimeout;
  this.lastUpdate = Date.now();
  this.update();
};

util.inherits(EndpointPool, Events.EventEmitter);

_.extend(EndpointPool.prototype, {
  update: function () {
    this.resolve(function (err, endpoints) {
      if (err || !endpoints || !endpoints.length) {
        this.emit('updateError', err, Date.now() - this.lastUpdate);
      } else {
        this.lastUpdate = Date.now();
        this.setEndpoints(endpoints);
      }
      this._updateTimeout = setTimeout(this.update.bind(this), this.ttl);
    }.bind(this));
  },

  resolve: function (cb) {
    var callback = _.once(cb);
    setTimeout(callback, DNS_LOOKUP_TIMEOUT, dns.TIMEOUT);
    dns.resolveSrv(this.discoveryName, callback);
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
      return new Endpoint(info, this.maxFailures, this.failureWindow, this.resetTimeout);
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

function Endpoint(info, maxFailures, failureWindow, resetTimeout) {
  this.name = info.name;
  this.port = info.port;
  this.url = info.name + ':' + info.port;
  this.state = CLOSED;
  this.failCount = 0;
  this.callback = endpointCallback.bind(this);
  this.disable = disableEndpoint.bind(this);
  this.resetTimeout = resetTimeout;
  this.failureWindow = failureWindow;
  // A ring buffer, holding the timestamp of each error. As we loop around the ring, the timestamp in the slot we're
  // about to fill will tell us the error rate. That is, `maxFailure` number of requests in how many milliseconds?
  this.buffer = new Array(maxFailures - 1);
  this.bufferPointer = 0;
}

function endpointCallback(err) {
  if (err) {
    var oldestErrorTime, now;
    if (this.state === OPEN) {
      return;
    }

    if (this.buffer.length === 0) {
      this.disable();
      return;
    }

    oldestErrorTime = this.buffer[this.bufferPointer];
    now = Date.now();
    this.buffer[this.bufferPointer] = now;
    this.bufferPointer++;
    this.bufferPointer %= this.buffer.length;

    if (this.state === HALF_OPEN_PENDING || (oldestErrorTime != null && now - oldestErrorTime <= this.failureWindow)) {
      this.disable();
    }
  } else if (this.state === HALF_OPEN_PENDING) {
    this.state = CLOSED;
  }
}

function disableEndpoint() {
  this.state = OPEN;
  clearInterval(this._reopenTimeout);
  this._reopenTimeout = setTimeout(function () {
    this.state = HALF_OPEN_READY;
  }.bind(this), this.resetTimeout);
}
