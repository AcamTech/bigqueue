var nslookup = require('nslookup');
var DNS_SERVER = '172.16.18.53';
var resolving = {};
var resolved = {}

function resolve(addr, cb) {
  if (resolved[addr]) {
    cb(null, randomAddress(resolved[addr]))
    return;
  }

  if (resolving[addr] && resolving[addr].length > 0) {
    resolving[addr].push(cb);
    return;
  }

  resolving[addr] = [cb];

  internalResolve(addr, function (err, addrs) {
    resolving[addr].forEach(function (callback) {
      try {
        callback(err, randomAddress(addrs));
      } catch (e) {
        console.error("error in lookup callback ", callback, resolving[addr], e);
      }
    });
    delete resolving[addr];
    setInterval(function () {
      internalResolve(addr);
    }, 30 * 1000);
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
