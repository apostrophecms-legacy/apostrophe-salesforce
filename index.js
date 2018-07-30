var _       = require('lodash'),
    async   = require('async'),
    flatten = require('flat'),
    moment  = require('moment'),
    jsforce = require('jsforce');

module.exports = salesforce;

function salesforce (options, callback) {
  return new Construct(options, callback);
}

function Construct (options, callback) {
  var self = this;

  var connection;

  // Add a bunch of methods to self here
  self._app = options.app;
  self._apos = options.apos;
  self._site = options.site;
  self.progress = self._apos.getCache('apostrophe-salesforce-progress');

  // The configuration of how Salesforce data maps into Apostrophe
  self.mappings = options.mappings;

  self._apos.db.collection('aposSalesforce', function (err, collection) {
    collection.findOne({}, {sort: [['$natural','desc']]}, function(err, doc) {
      if (!doc) return
      self.lastRun = doc.lastRun;
    });
  });

  self._app.get("/apos/salesforce/sync", function(req, res) {
    req.jobId = self._apos.generateId();
    return self.progress.set(req.jobId, { finished: false, error: false }, function(err) {
      res.redirect('/apos/salesforce/progress?jobId=' + req.jobId);
      // Deliberately continuing after res.redirect
      if (!err) {
        self.sync(req, req.query, function(err) {
          if (err) {
            self.progress.set(req.jobId, { finished: true, error: true }, function() {});
          } else {
            self.progress.set(req.jobId, { finished: true, error: false }, function() {});
          }
        });
      }
    });
  });

  self._app.get("/apos/salesforce/progress", function(req, res) {
    var jobId = req.query.jobId;
    return self.progress.get(jobId, function(err, obj) {
      if (err || (!obj)) {
        return respond({ error: true });
      } else {
        return respond(obj);
      }
    });
    function respond(obj) {
      return res.send(self.renderPage(req, 'progress', obj));
    }
  });

  // Salesforce authentication stuffs
  self.getUsername = function(options, callback) {
    callback(options.sfUsername);
  }

  self.getPassword = function(options, callback) {
    callback(options.sfPassword);
  }

  self.getSecurityToken = function(options, callback) {
    callback(options.sfSecurityToken);
  }

  // If options.resync is true, re-syncs everything, not just things modified
  // since the last run. Useful if the mappings have changed
  self.sync = function(req, options, callback) {
    if (!callback) {
      callback = options;
      options = {};
    }
    var startTime = moment().format();

    getConnection(function(err) {
      if (err) return console.log(err);

      // Leaving this in as a handy way to get a salesforce schema
      // connection.sobject("Contact").describe(function(err, res) {
      //   console.log(res);
      // });
      // return callback()

      var queries = [];

      // Create each query object
      self.mappings.forEach(function(mapping) {
        mapping.Type = self._site.modules[mapping.aposObj];
        mapping.req = req; // getOne and putOne requires req object
        var lastRun;
        if (options.resync) {
          lastRun = undefined;
        } else {
          lastRun = self.lastRun;
        }
        queries.push(new Query(mapping, lastRun));
      });
      return async.series({
          execute: function (callback) {
            return async.parallel(_.pluck(queries, 'execute'), callback);
          },
          map: function (callback) {
            return async.parallel(_.pluck(queries, 'map'), callback);
          },
          save: function (callback) {
            return async.parallel(_.pluck(queries, 'save'), callback);
          },
          join: function (callback) {
            return async.parallel(_.pluck(queries, 'join'), callback);
          }
        }, 
        function(err) {
          if (err) {
            return callback(err);
          }
          self.lastRun = startTime;
          console.log("Finished sync, logging last run");
          self._apos.db.collection('aposSalesforce', function (err, collection) {
            collection.insert({lastRun: self.lastRun, finished: new Date()}, function(err, result) {
              // Failure to insert lastRun is not an error
              return callback(null);
            });
          });
        }
      );
    })
  }

  function getConnection (callback) {
    connection = null;
    async.series({
      getUsername: function(callback) {
        self.getUsername(options, function(username) {
          self.sfUsername = username;
          callback();
        });
      },
      getPassword: function(callback) {
        self.getPassword(options, function(password) {
          self.sfPassword = password;
          callback();
        });
      },
      getSecurityToken: function(callback) {
        // Append security token to password, as Salesforce requires
        self.getSecurityToken(options, function(token) {
          self.sfPassword += token || "";
          callback();
        });
      }
    }, function() {
      connection = new jsforce.Connection({});
      connection.login(self.sfUsername, self.sfPassword, function(err, userInfo) {
        return callback(err, connection);
      });
    }); 
  }

  var Query = function (mapping, lastRun) {
    var self = this;

    var Type = mapping.Type;
    var req = mapping.req;

    var MAX_RESULTS = 20000;

    // Construct the SOQL Query
    var queryFields = [];
    var whereClauses = [];
    // Add delta logic
    if (lastRun) {
      whereClauses.push("LastModifiedDate > " + lastRun);
    }
    // Add fields to SELECT
    for(var aposField in mapping.fields) {
      var sfFields = mapping.fields[aposField];
      if(!(sfFields instanceof Array)) {
        sfFields = [sfFields];
      }
      queryFields.push.apply(queryFields, sfFields);
      // Add required fields
      if(mapping.required && mapping.required.indexOf(aposField) >= 0) {
        sfFields.forEach(function(sfField) {
          whereClauses.push(sfField + " != null")
        });
      }
    }
    // Add nested queries for JOINs
    for(var aposJoin in mapping.joins) {
      var join = mapping.joins[aposJoin];
      if (join.hasMany) {
        queryFields.push('(SELECT Entity.Id FROM ' + join.sfType + ' AS Entity)');
      } else {
        queryFields.push(join.sfType + '.Id');
      }
    }
    // Add custom WHERE clauses
    if (mapping.where) {
      whereClauses.push.apply(whereClauses, mapping.where)
    }
    // Create query string
    self.queryString = "SELECT Id, " + queryFields.join(', ') + 
                      " FROM " + mapping.sfObj + 
                      ((whereClauses.length > 0) ? " WHERE " + whereClauses.join(" AND ") : "") + 
                      " LIMIT " + MAX_RESULTS;

    self.sfResults = [];
    self.aposResults = [];

    // Executes the constructed query
    self.execute = function(callback) {
      connection.query(self.queryString)
        .on("record", function(sfObj) {
          sfObj = flatten(sfObj, {safe: true});
          self.sfResults.push(sfObj);
        })
        .on("end", function(query) {
          callback();
        })
        .on("error", function(err) {
          callback(err);
        })
        .run({autoFetch: true, maxFetch: MAX_RESULTS});
    }

    // Maps the salesforce results to apostrophe objects
    self.map = function(callback) {
      self.sfResults.forEach(function(sfObj) {
        // Create new instance of object and associate with Salesforce id
        var aposObj = {};
        aposObj.sfId = sfObj.Id;
        // Add fields to Apostrophe object according to mapping configuration
        for(var aposField in mapping.fields) {
          var sfFields = mapping.fields[aposField];
          if(!(sfFields instanceof Array)) {
            sfFields = [sfFields];
          }
          var sfFieldValues = [];
          sfFields.forEach(function(sfField) {
            var sfValue = sfObj[sfField];
            if(typeof sfValue !== 'undefined' && sfValue !== null) {
              sfFieldValues.push(sfValue);
            }
          });
          // Concatenate selected fields and add them to Apostrophe object
          if(sfFieldValues.length > 0) {
            aposObj[aposField] = sfFieldValues.join(' ');
          }
        }
        for(var aposArray in mapping.arrays) {
          var sfFields = mapping.arrays[aposArray];
          if(!(sfFields instanceof Array)) {
            sfFields = [sfFields];
          }
          var sfFieldValues = [];
          sfFields.forEach(function(sfField) {
            var sfValue = sfObj[sfField];
            if(typeof sfValue !== 'undefined' && sfValue !== null) {
              sfFieldValues.push(sfValue);
            }
          });
          if(sfFieldValues.length > 0) {
            aposObj[aposArray] = sfFieldValues;
          }
        }
        self.aposResults.push(aposObj);
      });
      callback();
    }

    // De-dups and saves the apostrophe objects
    self.save = function(callback) {
      // De-duping step
      // A reasonable amount of parallelism only, so we don't kill the site while
      // this is going on. -Tom
      async.eachLimit(self.aposResults, 3,
        function(aposObj, callback) {
          Type.getOne(req, {sfId: aposObj.sfId}, {}, function(err, item) {
            if(err) return callback(err);

            if(!item) {
              item = Type.newInstance();
            }
            item = _.extend(item, aposObj);
            //console.log(item.sfId + " -+- " + item.title);
            // Save the Apostrophe object
            Type.putOne(req, {}, item, function(err) {
              if (err) console.log(err);
              callback(err);
            });
          });
        }, function(err) {
          callback(err);
        }
      );
    }

    // Creates joins in the saved apostrophe objects
    self.join = function(callback) {
      if (!mapping.joins || mapping.joins.length <= 0) {
        return callback();
      }
      // A reasonable amount of parallelism only, so we don't kill the site while
      // this is going on. -Tom
      return async.eachLimit(self.sfResults, 3,
        function(sfObj, callback) {
          Type.getOne(req, {sfId: sfObj.Id}, {}, function(err, aposObj) {
            if(!aposObj) return callback();
            for(var aposJoin in mapping.joins) {
              var join = mapping.joins[aposJoin];
              if (join.hasMany) {
                var joinResults = sfObj[join.sfType + '.records'];
                if (!joinResults) return callback(err);
                // We're parallel enough at this point
                return async.eachSeries(joinResults, 
                  function(joinResult, callback) {
                  options.site.modules[join.aposType].getOne(req, {sfId: joinResult.Id}, {}, function(err, item) {
                    if(!item) return callback(err);
                    if(!aposObj[aposJoin]) {
                      aposObj[aposJoin] = [];
                    }
                    if (!_.contains(aposObj[aposJoin], item._id)) {
                      aposObj[aposJoin].push(item._id);
                    }
                    return callback(err);
                  });
                },
                function(err) {
                  Type.putOne(req, { version: false }, aposObj, function(err) {
                    return callback(err);
                  });
                });
              } else {
                var sfObjId = sfObj[join.sfType + ".Id"];
                options.site.modules[join.aposType].getOne(req, {sfId: sfObjId}, {}, function(err, item) {
                  if(!item) return callback(err);
                  aposObj[aposJoin] = item._id;
                  Type.putOne(req, { version: false }, aposObj, function(err) {
                    return callback(err);
                  });
                });
              }
            }
          });
        }, function(err) {
          return callback(err);
        }
      );
    }
  }

  // Add admin UI elements
  self._apos.mixinModuleAssets(self, 'salesforce', __dirname, options);
  self._apos.addLocal('aposSalesforceMenu', function(args) {
    var result = self.render('menu', args);
    return result;
  });

  self._apos.on('tasks:register', function(taskGroups) {
    taskGroups.apostrophe.salesforceSync = function(apos, argv, callback) {
      return self.sync(self._apos.getTaskReq(), {
        resync: argv.resync
      }, callback);
    };
  });
  
  // Ensure index on Salesforce id in Apostrophe
  self._apos.pages.ensureIndex({ sfId: 1 }, { safe: true }, function() {
    // Invoke the callback. This must happen on next tick or later!
    if (callback) {
      return process.nextTick(function() {
        return callback(null);
      });
    }
  });
  
}

// Export the constructor so others can subclass
salesforce.Construct = Construct;
