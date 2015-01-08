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

  // The configuration of how Salesforce data maps into Apostrophe
  self.mappings = options.mappings;

  // Salesforce authentication stuffs
  self.sfUsername = options.sfUsername;
  self.sfPassword = options.sfPassword;
  // Append security token to password, as Salesforce requires
  self.sfPassword += options.sfSecurityToken || "";

  self._apos.db.collection('aposSalesforce', function (err, collection) {
    collection.findOne({}, {sort: [['$natural','desc']]}, function(err, doc) {
      if (!doc) return
      self.lastRun = doc.lastRun;
    });
  });

  self._app.get("/apos/salesforce/sync", function(req, res) {
    self.sync(req, function() {
      console.log("done syncing");
    });
    res.redirect('/');
  });

  self.sync = function(req, callback) {
    var startTime = moment().format();

    getConnection(function(err) {
      if (err) return console.log(err);

      // Leaving this in as a handy way to get a salesforce schema
      // connection.sobject("Project_Volunteer__c").describe(function(err, res) {
      //   console.log(res);
      // })
      // return

      var queries = [];

      // Create each query object
      self.mappings.forEach(function(mapping) {
        mapping.Type = self._site.modules[mapping.aposObj];
        mapping.req = req; // getOne and putOne requires req object
        queries.push(new Query(mapping, self.lastRun));
      });
      async.series({
          execute: function (callback) {
            async.parallel(_.pluck(queries, 'execute'), function () {
              callback();
            });
          },
          map: function (callback) {
            async.parallel(_.pluck(queries, 'map'), function () {
              callback();
            });
          },
          save: function (callback) {
            async.parallel(_.pluck(queries, 'save'), function () {
              callback();
            });
          },
          join: function (callback) {
            async.parallel(_.pluck(queries, 'join'), function () {
              callback();
            });
          }
        }, 
        function() {
          self.lastRun = startTime;
          console.log("Finished sync, logging last run");
          self._apos.db.collection('aposSalesforce', function (err, collection) {
            collection.insert({lastRun: self.lastRun, finished: new Date()}, function(err, result) {
              return callback();
            });
          });
        }
      );
    })
  }

  function getConnection (callback) {
    if (!connection) {
      connection = new jsforce.Connection({});
      connection.login(self.sfUsername, self.sfPassword, function(err, userInfo) {
        return callback(err, connection);
      });
    } else {
      return callback(null, connection);
    }
  }

  var Query = function (mapping, lastRun) {
    var self = this;

    var Type = mapping.Type;
    var req = mapping.req;

    var MAX_RESULTS = 10000;

    // Construct the SOQL Query
    var queryFields = [];
    var whereClauses = [];
    // Add delta logic
    if(lastRun) {
      whereClauses.push("LastModifiedDate > " + lastRun);
    }
    // Add fields to SELECT
    for(aposField in mapping.fields) {
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
    for(aposJoin in mapping.joins) {
      var join = mapping.joins[aposJoin];
      queryFields.push('(SELECT Entity.Id FROM ' + join.sfType + ' AS Entity)');
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
      console.log("execute---" + self.queryString);
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
        var aposObj = Type.newInstance();
        aposObj.sfId = sfObj.Id;
        // Add fields to Apostrophe object according to mapping configuration
        for(aposField in mapping.fields) {
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
          self.aposResults.push(aposObj);
        }
      });
      callback();
    }

    // De-dups and saves the apostrophe objects
    self.save = function(callback) {
      // De-duping step
      async.each(self.aposResults, 
        function(aposObj, callback) {
          Type.getOne(req, {sfId: aposObj.sfId}, {}, function(err, item) {
            if(err) return callback(err);
            if(!item) {
              item = aposObj;
            } else {
              _.extend(item, aposObj);
            }
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
      async.each(self.sfResults,
        function(sfObj, callback) {
          Type.getOne(req, {sfId: sfObj.Id}, {}, function(err, aposObj) {
            if(!aposObj) return callback();
            if(!aposObj[aposJoin]) {
              aposObj[aposJoin] = [];
            }
            for(aposJoin in mapping.joins) {
              var join = mapping.joins[aposJoin];
              var joinResults = sfObj[join.sfType + '.records'];

              if (!joinResults) return callback(err);
                async.each(joinResults, 
                  function(joinResult, callback) {
                  options.site.modules[join.aposType].getOne(req, {sfId: joinResult.Id}, {}, function(err, item) {
                    if(!item) return callback(err);
                    //console.log(sfObj.Name + "---" + (item ? item.title + " " + item._id : "none"));
                    if (!_.contains(aposObj[aposJoin], item._id)) {
                      aposObj[aposJoin].push(item._id);
                    }
                    return callback(err);
                  });
                },
                function(err) {
                  //console.log(aposObj);
                  Type.putOne(req, {}, aposObj, function(err) {
                    return callback(err);
                  });
                }
              );
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

  self._apos.tasks.salesforceSync = function(callback) {
    self.sync(self._apos.getTaskReq(), function() {
      console.log("Salesforce Sync is finshed.");
      return callback(null);
    });
  }
  
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
