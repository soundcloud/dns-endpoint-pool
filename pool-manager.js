var _ = require('underscore');
// endpoint states
var CLOSED            = 0;  // closed circuit: endpoint is good to use
var HALF_OPEN_READY   = 1;  // endpoint is in recovery state: offer it for use once
var HALF_OPEN_PENDING = 2;  // endpoint recovery is in process
var OPEN              = 3;  // open circuit: endpoint is no good

function PoolManager (options) {
  options = options || {};

  this.endpoints = [];
  this._endpointOffset = 0;

  this.isInPool = options.isInPool || _.constant(true);
  this.onEndpointReturned = options.onEndpointReturned || _.noop;
  this.onEndpointRegistered = options.onEndpointRegistered || _.noop;
  this.onEndpointSelected = options.onEndpointSelected || _.noop;
}

PoolManager.prototype = {
  hasEndpoints: function () {
    return this.endpoints.length > 0;
  },
  getNextEndpoint: function () {
    var i;
    var l;
    var offset;
    var endpoint;

    for (i = 0, l = this.endpoints.length; i < l; ++i) {
      offset = (this._endpointOffset + i) % l;
      endpoint = this.endpoints[offset];

      if (this.isInPool(endpoint)) {
        this.onEndpointSelected(endpoint);
        this._endpointOffset = offset + 1;
        return endpoint;
      }
    }
  },
  updateEndpoints: function (endpoints) {
    var matchingEndpoint;
    var i;
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
  },
  getStatus: function () {
    var manager = this;
    return {
      total: this.endpoints.length,
      unhealthy: this.endpoints.reduce(function (badCount, endpoint) {
        return badCount + (manager.isInPool(endpoint) ? 0 : 1);
      }, 0)
    };
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
    if (!options) {
      throw new Error('Must supply arguments to ejectOnErrorPoolManager');
    }

    var poolConfig;
    if (options.failureWindow && options.maxFailures && options.resetTimeout) {
      poolConfig = getRollingWindowConfiguration(options.failureWindow, options.maxFailures, options.resetTimeout);
    } else if (options.failureRate && options.failureRateWindow && options.resetTimeout) {
      poolConfig = getRateConfiguration(options.failureRate, options.failureRateWindow, options.resetTimeout);
    } else {
      throw new Error('Must supply either configuration to ejectOnErrorPoolManager');
    }

    return new PoolManager(poolConfig);

    function disableEndpoint(endpoint) {
      if (endpoint.state === OPEN) {
        return;
      }
      endpoint.state = OPEN;
      clearInterval(endpoint._reopenTimeout);
      endpoint._reopenTimeout = setTimeout(function () {
        endpoint.state = HALF_OPEN_READY;
      }, options.resetTimeout);
    }
    function isInPool(endpoint) {
      return endpoint.state === CLOSED || endpoint.state === HALF_OPEN_READY;
    }
    function onEndpointSelected(endpoint) {
      if (endpoint.state === HALF_OPEN_READY) {
        endpoint.state = HALF_OPEN_PENDING; // let one through, then turn it off again
      }
    }

    function getRollingWindowConfiguration(failureWindow, maxFailures, resetTimeout) {
      return {
        isInPool: isInPool,
        onEndpointSelected: onEndpointSelected,
        onEndpointRegistered: function (endpoint) {
          endpoint.state = CLOSED;
          // A ring buffer, holding the timestamp of each error. As we loop around the ring, the timestamp in the slot we're
          // about to fill will tell us the error rate. That is, `maxFailure` number of requests in how many milliseconds?
          endpoint.buffer = new RingBuffer(maxFailures - 1);
        },
        onEndpointReturned: function (endpoint, err) {
          if (err) {
            if (endpoint.state === OPEN) {
              return;
            }

            if (endpoint.buffer.size === 0) {
              disableEndpoint(endpoint);
              return;
            }

            var now = Date.now();
            var oldestErrorTime = endpoint.buffer.read();
            endpoint.buffer.write(now);

            if (endpoint.state === HALF_OPEN_PENDING || (oldestErrorTime != null && now - oldestErrorTime <= failureWindow)) {
              disableEndpoint(endpoint);
            }
          } else if (endpoint.state === HALF_OPEN_PENDING) {
            endpoint.state = CLOSED;
          }
        }
      };
    }

    function getRateConfiguration(failureRate, failureRateWindow, resetTimeout) {
      var maxErrorCount = failureRate * failureRateWindow;
      return {
        isInPool: isInPool,
        onEndpointSelected: onEndpointSelected,
        onEndpointRegistered: function (endpoint) {
          endpoint.state = CLOSED;
          endpoint.buffer = new RingBuffer(failureRateWindow);
          endpoint.errors = 0;
        },
        onEndpointReturned: function (endpoint, err) {
          var state = endpoint.state;
          var newStatus = err ? 1 : 0;
          var oldestStatus = endpoint.buffer.read() ? 1 : 0;
          endpoint.buffer.write(newStatus);
          endpoint.errors += newStatus - oldestStatus;

          if (err && (state === HALF_OPEN_PENDING || endpoint.errors >= maxErrorCount)) {
            disableEndpoint(endpoint);
          } else if (!err && state === HALF_OPEN_PENDING) {
            endpoint.state = CLOSED;
          }
        }
      };
    }
  }
};

function RingBuffer(size) {
  this.buffer = new Array(size);
  this.offset = 0;
  this.size = size;
}
_.assign(RingBuffer.prototype, {
  read: function () {
    return this.buffer[this.offset];
  },
  write: function (val) {
    this.buffer[this.offset] = val;
    this.offset = (this.offset + 1) % this.size;
  }
});
