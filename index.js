var EndpointPool,
    _           = require('underscore'),
    dns         = require('dns'),
    Events      = require('events'),
    PoolManager = require('./pool-manager'),
    util        = require('util'),

    DNS_LOOKUP_TIMEOUT = 1000;

/**
 * @param {String}    discoveryName       The name of the service discovery host
 * @param {Number}    ttl                 How long the endpoints are valid for. The service discovery endpoint will be checked on
 *                                        this interval.
 * @param {{maxFailures: Number, failureWindow: Number, resetTimeout: Number}}
 *                    ejectOnErrorConfig  How to handle endpoint errors. If specified, the following options must be defined:
 *                                        - maxFailures: Number of failures allowed before the endpoint circuit breaker is tripped.
 *                                        - failureWindow: Size of the sliding window of time in which the failures are counted.
 *                                        - resetTimeout: Amount of time before putting the circuit back into half open state.
 * @param {Function=} onReady       Callback to execute when endpoints have been primed (updated for the first time)
 */
module.exports = EndpointPool = function (discoveryName, ttl, ejectOnErrorConfig, onReady) {
  if (!discoveryName || !ttl) {
    throw new Error('Must supply all arguments');
  }

  if (ejectOnErrorConfig) {
    this.poolManager = PoolManager.ejectOnErrorPoolManager(ejectOnErrorConfig);
  } else {
    this.poolManager = PoolManager.defaultPoolManager();
  }

  Events.EventEmitter.call(this);

  this.discoveryName = discoveryName;
  this.ttl = ttl;
  this._updateTimeout = null;

  this.lastUpdate = Date.now();
  this.update(onReady);
};

util.inherits(EndpointPool, Events.EventEmitter);

_.extend(EndpointPool.prototype, {
  update: function (onDone) {
    this.resolve(function (err, endpoints) {
      if (err || !endpoints || !endpoints.length) {
        this.emit('updateError', err, Date.now() - this.lastUpdate);
      } else {
        this.lastUpdate = Date.now();
        this.setEndpoints(endpoints);
      }
      this._updateTimeout = setTimeout(this.update.bind(this), this.ttl);

      if (typeof onDone === 'function') {
        onDone();
      }
    }.bind(this));
  },

  resolve: function (cb) {
    var callback = _.once(cb);
    setTimeout(callback, DNS_LOOKUP_TIMEOUT, dns.TIMEOUT);
    dns.resolveSrv(this.discoveryName, callback);
  },

  getEndpoint: function () {
    var endpoint = this.poolManager.getNextEndpoint();

    if (endpoint) {
      return endpoint;
    } else {
      this.emit('noEndpoints');
      return null;
    }
  },

  hasEndpoints: function () {
    return this.poolManager.hasEndpoints();
  },

  setEndpoints: function (endpoints) {
    this.poolManager.updateEndpoints(endpoints);
  },

  stopUpdating: function () {
    clearTimeout(this._updateTimeout);
  }
});
