var _ = require('underscore');

function PoolManager (options) {
  options = options || {};

  this.endpoints = [];
  this._endpointOffset = 0;

  this.isInPool = options.isInPool || _.constant(true);
  this.onEndpointReturned = options.onEndpointReturned || _.noop;
  this.onEndpointRegistered = options.onEndpointRegistered || _.noop;
}

PoolManager.prototype = {
  hasEndpoints: function () {
    return this.endpoints.length > 0;
  },
  getNextEndpoint: function () {
    var i, l, offset, endpoint;

    for (i = 0, l = this.endpoints.length; i < l; ++i) {
      offset = (this._endpointOffset + i) % l;
      endpoint = this.endpoints[offset];

      if (this.isInPool(endpoint)) {
        this._endpointOffset = offset + 1;
        return endpoint;
      }
    }
  },
  updateEndpoints: function (endpoints) {
    var matchingEndpoint, i;
    var newEndpoints = endpoints.map(function (info) {
      return new Endpoint(info);
    });

    for (i = this.endpoints.length; i--;) {
      matchingEndpoint = _.findWhere(newEndpoints, { url: this.endpoints[i].url });

      if (matchingEndpoint) { // found a match, remove it from `newEndpoints`, since it's not new
        newEndpoints = _.without(newEndpoints, matchingEndpoint);
      } else { // didn't find a match in endpoints, so kill that endpoint
        this.endpoints.splice(i, 1);
      }
    }

    newEndpoints.forEach(function (endpoint) {
      endpoint.callback = this.onEndpointReturned.bind(this, endpoint);
      this.onEndpointRegistered(endpoint);
    }, this);
    // push all the actually-new endpoints in
    this.endpoints.push.apply(this.endpoints, newEndpoints);
  }
};

function Endpoint(info) {
  this.name = info.name;
  this.port = info.port;
  this.url = info.name + ':' + info.port;
}

module.exports = {
  defaultPoolManager: function () {
    return new PoolManager();
  },
  ejectOnErrorPoolManager: function (options) {
    // endpoint states
    var CLOSED            = 0,  // closed circuit: endpoint is good to use
        HALF_OPEN_READY   = 1,  // endpoint is in recovery state: offer it for use once
        HALF_OPEN_PENDING = 2,  // endpoint recovery is in process
        OPEN              = 3;  // open circuit: endpoint is no good

    if (!options || !(options.failureWindow && options.maxFailures && options.resetTimeout)) {
      throw new Error('Must supply all arguments to ejectOnErrorPoolManager');
    }

    var failureWindow = options.failureWindow;
    var maxFailures = options.maxFailures;
    var resetTimeout = options.resetTimeout;

    function disableEndpoint(endpoint) {
      endpoint.state = OPEN;
      clearInterval(endpoint._reopenTimeout);
      endpoint._reopenTimeout = setTimeout(function () {
        endpoint.state = HALF_OPEN_READY;
      }, resetTimeout);
    }

    return new PoolManager({
      isInPool: function (endpoint) {
        switch (endpoint.state) {
          case HALF_OPEN_READY:
            endpoint.state = HALF_OPEN_PENDING; // let one through, then turn it off again
            /* falls through */
          case CLOSED:
            return true;
          default:
            return false;
        }
      },
      onEndpointRegistered: function (endpoint) {
        endpoint.state = CLOSED;
        endpoint.failCount = 0;
        // A ring buffer, holding the timestamp of each error. As we loop around the ring, the timestamp in the slot we're
        // about to fill will tell us the error rate. That is, `maxFailure` number of requests in how many milliseconds?
        endpoint.buffer = new Array(maxFailures - 1);
        endpoint.bufferPointer = 0;
      },
      onEndpointReturned: function (endpoint, err) {
        if (err) {
          var oldestErrorTime, now;
          if (endpoint.state === OPEN) {
            return;
          }

          if (endpoint.buffer.length === 0) {
            disableEndpoint(endpoint);
            return;
          }

          oldestErrorTime = endpoint.buffer[endpoint.bufferPointer];
          now = Date.now();
          endpoint.buffer[endpoint.bufferPointer] = now;
          endpoint.bufferPointer++;
          endpoint.bufferPointer %= endpoint.buffer.length;

          if (endpoint.state === HALF_OPEN_PENDING || (oldestErrorTime != null && now - oldestErrorTime <= failureWindow)) {
            disableEndpoint(endpoint);
          }
        } else if (endpoint.state === HALF_OPEN_PENDING) {
          endpoint.state = CLOSED;
        }
      }
    })
  }
};
