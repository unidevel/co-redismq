'use strict';
var process = require('process');
var co = require('co');
const state = {
  NEW: 0,
  RUNNING: 1,
  FAILED : 2,
  DONE   : 100
}
var util = require('./util');

function callFunction(callback, value, done){
  var handled = false;
  function doneWithFlag(err, value){
    handled = true;
    done(err, value);
  }
  if ( util.isGeneratorFunction(callback) ){
    co(function* callFunction(){
      var result = yield callback(value);
      doneWithFlag(null, result);
    }).catch(doneWithFlag);
  }
  else {
    try {
      callback.apply(null, [value, doneWithFlag]);
    }
    catch(err){
      doneWithFlag(err);
    }
  }
  if ( !handled ) done();
}

function watchQueue(poolClient, redis, handle){
  var queue = handle.queue;
  var jobKey = util.JOBKEY(queue);
  var processKey = util.PROCESSKEY(queue);
  poolClient.brpoplpush(jobKey, processKey, 0, function(err, id){
    if ( err ) console.error(err);
    else {
      processQueue(redis, id, handle);
    }
    process.nextTick(watchQueue.bind(null, poolClient, redis, handle));
  });
}

function processQueue(redis, id, handle){
  var queue = handle.queue;
  var dataKey = util.DATAKEY(queue);
  var statusKey = util.STATUSKEY(queue);
  var processKey = util.PROCESSKEY(queue);
  var status = {
    state: state.RUNNING,
    ts   : new Date().getTime()
  }
  function done(err){
    if ( err ) console.error('Failed job for queue['+queue+'] with id['+id+']', err);
    var status = {
      state: err?state.FAILED: state.DONE,
      ts   : new Date().getTime()
    }
    redis.multi()
      .hset(statusKey, id, util.encode(status), util.dummy)
      .lrem(processKey, 1, id)
      .exec(util.dummy);
  }
  redis.hset(statusKey, id, util.encode(status), util.dummy);
  redis.hget(dataKey, id, function(err, data){
    if ( data == null ) {
      console.warn('Job['+id+'] data in queue['+queue+'] has been deleted!');
      return;
    }
    var decodedData = util.decode(data);
    var job = {
      id: id,
      data: decodedData
    }
    callFunction(handle.callback, job, done);
  });
}

class RestMQ {
  constructor(args){
    var opts = args || {};
    this.queue = null;
    this.client = null;
    this.options = opts;
    this.redis = util.createClient(opts.redis);
  }

  *push(queue, data, expire){
    if ( !data ) throw new Error('Job data is required!');
    var id = util.generateId();
    var encodedData = util.encode(data);
    var dataKey = util.DATAKEY(queue);
    var jobKey = util.JOBKEY(queue);
    var statusKey = util.STATUSKEY(queue);
    var redis = this.redis;
    var status = {
      state: state.NEW,
      ts: new Date().getTime()
    }
    var multi = redis.multi();
    yield util.trunk0(multi,
      multi.hset(dataKey, id, encodedData)
        .hset(statusKey, id, util.encode(status))
        .lpush(jobKey, id)
      .exec);
    return id;
  }

  *status(id){
    var queue = this.name;
    var statusKey = util.STATUSKEY(queue);
    var data = yield util.thunk2(this.redis, this.redis.hget, statusKey, id);
    var status = util.decode(data);
    return status;
  }

  *remove(id){
    var dataKey = util.DATAKEY(queue);
    var jobKey = util.JOBKEY(queue);
    var statusKey = util.STATUSKEY(queue);
    var multi = redis.multi();
    yield util.trunk0(multi,
      multi.hdel(dataKey, id)
        .hdel(statusKey, id)
        .exec
    );
  }

  process(queue, callback, errorCallback){
    var watchRedis = util.createClient(this.options);
    var handle = {
      queue: queue,
      callback: callback
    }
    watchQueue(watchRedis, this.redis, handle);
  }
}

function createMQ(opts) {
  return new RestMQ(opts);
}

for ( var key in state ) {
  createMQ[key] = createMQ[key];
}

module.exports = createMQ;
