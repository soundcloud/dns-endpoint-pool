/*globals it, describe, beforeEach, afterEach */
var expect = require('expect.js'),
    Sinon = require('sinon'),
    DEP = require('./');

describe('DNS Endpoint Pool', function () {
  var stubs = [], clock;
  function autoRestore(stub) {
    stubs.push(stub);
    return stub;
  }

  beforeEach(function () {
    clock = autoRestore(Sinon.useFakeTimers());
  });

  afterEach(function () {
    var stub;
    while ((stub = stubs.pop())) {
      stub.restore();
    }
  });

  it('enforces that all arguments are passed to the constructor', function () {
    autoRestore(Sinon.stub(DEP.prototype, 'update'));
    [
      function () { return new DEP(); },
      function () { return new DEP('foo.localhost'); }
    ].forEach(function (fn) {
      expect(fn).to.throwError('Must supply all arguments');
    });
  });

  it('will automatically begin updating when constructed', function () {
    var stub = autoRestore(Sinon.stub(DEP.prototype, 'update')),
        dep = new DEP('foo.localhost', 5000);
    Sinon.assert.calledOnce(stub);
  });

  it('will execute a callback after the first update', function () {
    var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
        called = false,
        dep;

    resolve.callsArgWith(0, null, []);
    dep = new DEP('foo.localhost', 5000, null, function () {
      called = true;
    });

    expect(called).to.be(true);

    dep.stopUpdating();
  });

  it('will update on a timer', function () {
    var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
        dep;

    resolve.callsArgWith(0, null, []);
    dep = new DEP('foo.localhost', 5000);

    Sinon.assert.calledOnce(resolve);

    clock.tick(5000);
    Sinon.assert.calledTwice(resolve);

    clock.tick(5000);
    Sinon.assert.calledThrice(resolve);

    dep.stopUpdating();
  });

  it('will add resolved endpoints and serve them in rotation', function () {
    var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
        dep;

    resolve.callsArgWith(0, null, [
      { name: 'bar.localhost', port: 8000 },
      { name: 'baz.localhost', port: 8001 }
    ]);
    dep = new DEP('foo.localhost', 5000);

    expect(dep.getEndpoint().url).to.be('bar.localhost:8000');
    expect(dep.getEndpoint().url).to.be('baz.localhost:8001');
    dep.stopUpdating();
  });

  it('will trigger an error if resolving fails', function () {
    var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
        errorData,
        dep;

    resolve
      .onFirstCall().callsArgWith(0, null, [
        { name: 'bar.localhost', port: 8000 },
        { name: 'baz.localhost', port: 8001 }
      ])
      .onSecondCall().callsArgWith(0, { error: true });

    dep = new DEP('foo.localhost', 5000);

    expect(dep.getEndpoint().url).to.be('bar.localhost:8000');

    dep.on('updateError', function (err) {
      errorData = err;
    });

    clock.tick(5000);
    expect(errorData).to.eql({ error: true });

    // but it reuses the previous hosts
    expect(dep.getEndpoint().url).to.be('baz.localhost:8001');
  });

  it('will update with new endpoints returned', function () {
    var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
        bazEndpoint,
        dep;

    resolve
      .onFirstCall().callsArgWith(0, null, [
        { name: 'bar.localhost', port: 8000 },
        { name: 'baz.localhost', port: 8001 }
      ])
      .onSecondCall().callsArgWith(0, null, [
        { name: 'baz.localhost', port: 8001  },
        { name: 'quux.localhost', port: 8002 }
      ]);
    dep = new DEP('foo.localhost', 5000);

    dep.getEndpoint(); // bar
    bazEndpoint = dep.getEndpoint();

    expect(bazEndpoint.url).to.be('baz.localhost:8001');

    clock.tick(5000);

    expect(dep.getEndpoint()).to.be(bazEndpoint);

    expect(dep.getEndpoint().url).to.be('quux.localhost:8002');

    dep.stopUpdating();
  });

  it('can query the state of endpoints', function () {
    var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
        dep;

    resolve
      .onFirstCall().callsArgWith(0, { error: true })
      .onSecondCall().callsArgWith(0, null, [ { name: 'bar.localhost', port: 8000 } ])
      .onThirdCall().callsArgWith(0, { error: true });

    dep = new DEP('foo.localhost', 5000);

    expect(dep.hasEndpoints()).to.be(false);
    clock.tick(5000);
    expect(dep.hasEndpoints()).to.be(true);
    clock.tick(5000);
    // should still have old endpoints
    expect(dep.hasEndpoints()).to.be(true);
  });

  describe('with eject-on-error pool management', function () {

    var ejectOnErrorConfig = {
      maxFailures: 2,
      failureWindow: 10000,
      resetTimeout: 10000
    };

    it('enforces that config object has proper shape', function () {
      autoRestore(Sinon.stub(DEP.prototype, 'update'));
      [
        function () { return new DEP('foo.localhost', 5000, { maxFailures: 2 }); },
        function () { return new DEP('foo.localhost', 5000, { maxFailures: 2, failureWindow: 10000 }); }
      ].forEach(function (fn) {
        expect(fn).to.throwError('Must supply all arguments to ejectOnErrorPoolManager');
      });
    });

    it('will remove endpoints from the pool if they fail', function () {
      var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
          barEndpoint,
          bazEndpoint,
          status,
          dep;

      resolve.callsArgWith(0, null, [
        { name: 'bar.localhost', port: 8000 },
        { name: 'baz.localhost', port: 8001 }
      ]);

      dep = new DEP('foo.localhost', 5000, ejectOnErrorConfig);

      barEndpoint = dep.getEndpoint();
      barEndpoint.callback(true);

      bazEndpoint = dep.getEndpoint();

      expect(dep.getEndpoint()).to.be(barEndpoint); // still in the pool
      barEndpoint.callback(true);

      expect(dep.getEndpoint()).to.be(bazEndpoint);
      expect(dep.getEndpoint()).to.be(bazEndpoint); // bar is removed

      status = dep.getStatus();
      expect(status.total).to.be(2);
      expect(status.unhealthy).to.be(1);
      expect(status.age).to.be.a('number');

      dep.stopUpdating();
    });

    it('will reinstate endpoints for a single request after a timeout', function () {
      var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
          barEndpoint,
          bazEndpoint,
          dep;

      resolve.callsArgWith(0, null, [
        { name: 'bar.localhost', port: 8000 },
        { name: 'baz.localhost', port: 8001 }
      ]);

      dep = new DEP('foo.localhost', 5000, ejectOnErrorConfig);

      barEndpoint = dep.getEndpoint();
      barEndpoint.callback(true);
      barEndpoint.callback(true); // removed from pool.

      bazEndpoint = dep.getEndpoint();

      clock.tick(10000);

      expect(dep.getEndpoint()).to.be(barEndpoint);
      expect(dep.getEndpoint()).to.be(bazEndpoint);
      expect(dep.getEndpoint()).to.be(bazEndpoint); // only return barEndpoint once

      barEndpoint.callback(null); // denotes success
      expect(dep.getEndpoint()).to.be(barEndpoint); // it's back in the game
      dep.stopUpdating();
    });

    it('reports the age of the endpoints when updates fail', function () {
      var resolve = autoRestore(Sinon.stub(DEP.prototype, 'resolve')),
          errorHandler = Sinon.spy(),
          errObj = { error: true },
          dep;

      // works on the first and fourth calls, fails every other time
      resolve
        .callsArgWith(0, errObj)
        .onFirstCall().callsArgWith(0, null, [
          { name: 'bar.localhost', port: 8000 },
          { name: 'baz.localhost', port: 8001 }
        ])
        .onCall(3).callsArgWith(0, null, [
          { name: 'bar.localhost', port: 8000 },
          { name: 'baz.localhost', port: 8001 }
        ]);

      dep = new DEP('foo.localhost', 5000, ejectOnErrorConfig);  // call 1
      dep.on('updateError', errorHandler);
      clock.tick(5000); // call 2
      clock.tick(5000); // call 3
      clock.tick(5000); // call 4, should reset the timer
      clock.tick(5000); // call 5

      Sinon.assert.calledThrice(errorHandler);

      expect(errorHandler.firstCall.calledWithExactly(errObj, 5000)).to.be(true);
      expect(errorHandler.secondCall.calledWithExactly(errObj, 10000)).to.be(true);
      expect(errorHandler.thirdCall.calledWithExactly(errObj, 5000)).to.be(true);
      dep.stopUpdating();
    });
  });

});
