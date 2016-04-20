/**
module.exports.kue = {
  connection: 'kue',
  prefix: 'kue:',
  options: {
    driftFactor: 0.01,
    retryCount:  3,
    retryDelay:  200
  }
};
module.exports.connections = {
  kue: {
    hosts: [{host: 'localhost', port: 6379}],
    password: 'xxxx'
  }
}
**/

'use strict';
var Promise = require('bluebird');
var kue = require('kue');

function lift (done) {
  var kueConfig = framework.config.kue;
  if(!kueConfig) {
    throw new Error('require kue config');
  }

  var connectionName = kueConfig.connection;
  if(!connectionName) {
    throw new Error('require kue connection name');
  }

  var connectionConfig = framework.config.connections[connectionName] || {};
  var queue = kue.createQueue({
    prefix: kueConfig.prefix,
    redis: {
      port: connectionConfig.port || 6379,
      host: connectionConfig.host || '127.0.0.1',
      auth: connectionConfig.password || ''
    }
  });

  framework.kue = {
    queue: queue,
    //create a new job and insert it into kue
    createJob: function (name, data, options, done) {
      options = options || {};
      var job = queue.create(name, data);

      if(options.delay) {
        job.delay(options.delay);
      }
      if(options.priority) {
        job.priority(options.priority);
      }
      if(options.attempts) {
        job.attempts(options.attempts);
      }
      if(options.backoff) {
        job.backoff(options.backoff);
      }
      options.ttl = options.ttl || 1000 * 60 * 60 * 24;
      if(options.ttl) {
        job.ttl(options.ttl);
      }

      job.save(function (err) {
        console.log('xxxxyyy', job);
        if(done) {
          done(err, job);
        }
      });
      return job;
    },
    queryJobs: function (name, state, dataFilter, limit, done) {
      kue.Job.rangeByType(name, state, 0, limit, 'asc', function (err, jobs) {
        if(err) {
          return done(err);
        }
        done(null, _.filter(jobs, function (job) {
          if(dataFilter) {
            return _.isEqual(_.pick(job.data, Object.keys(dataFilter)), dataFilter);
          }
          return true;
        }));
      });
    },
    getJob: function (id, done) {
      kue.Job.get(id, done);
    },
    processJob: function (name, options, callback) {
      if(options instanceof Function) {
        callback = options;
        options = {};
      }
      options = options || {};

      var concurrency = options.concurrency || 1;

      queue.process(name, concurrency, function (job, done) {
        callback(job, done);
      });
    },

    clear: function (done) {
      kue.Job.rangeByState('complete', 0, 10000, 'asc', function (err, jobs) {
        if(err) {
          return done(err);
        }
        async.each(jobs, function (job, done) {
          job.remove(done);
        }, done);
      });
    }
  };

  done();
};

module.exports = Promise.promisify(lift);
