var nslookup = require('nslookup');
var DNS_SERVER = '172.16.18.53';
var resolveIntervals = {};
var resolved = {}

function resolve(addr, cb) {
  if (resolved[addr]) {
    cb(null, randomAddress(resolved[addr]))
    return;
  }

  internalResolve(addr, function (err, addrs) {
    if (!resolveIntervals[addr]) {
      resolveIntervals[addr] = setInterval(function () {
        internalResolve(addr);
      }, 30 * 1000);
    }
    return cb(err, randomAddress(addrs));
  });
}

function internalResolve(addr, cb) {
  nslookup(addr)
    .server(DNS_SERVER)
    .timeout(1000)
    .end(function (err, addrs) {
      if (!err) {
        resolved[addr] = addrs.filter(function (e) { return e; });
      }
      if (cb) {
        cb(err, resolved[addr]);
      }
    });
}

function randomAddress(addrs) {
  if (!addrs) {
    return null;
  }
  return addrs[Math.floor(Math.random() * addrs.length)];
}

module.exports = resolve;
