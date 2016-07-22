var process = require('process');
var redis = require('redis');
const RADIX = 36;
const RANDBASE = RADIX*RADIX*RADIX*RADIX;
const RANDBASE2 = RANDBASE*RANDBASE;

exports.generateId = function generateId(len){
  var ts = new Date().getTime();
  var pid = process.pid;
  var rand = parseInt(Math.random()*RANDBASE);
  var id = rand.toString(RADIX) + ts.toString(RADIX) + pid.toString(RADIX);
  while ( id.length < len ) {
    id += parseInt(Math.random()*RANDBASE2).toString(RADIX);
  }
  return id.substr(0, len);
}

exports.channelName = function channelName(name){
  return 'redismq:channel:'+name;
}

exports.eventName = function eventName(){
  return 'redismq:event:'+name;
}

exports.encode = function encodeData(data){
  return JSON.stringify(data);
}

exports.decode = function decodeData(data){
  return JSON.parse(data);
}

exports.DATAKEY = function DATAKEY(name){
  return 'redismq:data:'+name;
}

exports.JOBKEY = function JOBKEY(name){
  return 'redismq:jobid:'+name;
}

exports.STATUSKEY = function STATUSKEY(name){
  return 'redismq:status:'+name;
}

exports.PROCESSKEY = function STATUSKEY(name){
  return 'redismq:process:'+name;
}

exports.trunk0 = function(obj, fn){
  return function trunk0(cb){
    fn.call(obj, cb);
  }
}

exports.trunk1 = function(obj, fn, arg1){
  return function trunk1(cb){
    fn.call(obj, arg1, cb);
  }
}

exports.trunk2 = function(obj, fn, arg1, arg2){
  return function trunk2(cb){
    fn.call(obj, arg1, arg2, cb);
  }
}

exports.trunk3 = function(obj, fn, arg1, arg2, args3){
  return function trunk3(cb){
    fn.call(obj, arg1, arg2, args3, cb);
  }
}

exports.parallel = function parallel(fn, n){
  return function parallel(cb){
    var count = 0;
    var err = null;
    function done(e, value){
      count ++;
      if ( !err ) err = e;
      if ( count == n ) {
        cb(err, value);
      }
    }
    fn(done);
  }
}

exports.dummy = function(err){
  if(err)console.error('DUMMY ERROR: ', err);
}

exports.createClient = function createRedisClient(opts){
  return redis.createClient(opts);
}

exports.isPromise = function isPromise(obj) {
  return 'function' == typeof obj.then;
}

exports.isGenerator = function isGenerator(obj) {
  return 'function' == typeof obj.next && 'function' == typeof obj.throw;
}

exports.isGeneratorFunction = function isGeneratorFunction(obj) {
  var constructor = obj.constructor;
  if (!constructor) return false;
  if ('GeneratorFunction' === constructor.name || 'GeneratorFunction' === constructor.displayName) return true;
  return this.isGenerator(constructor.prototype);
}
