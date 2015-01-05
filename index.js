var _       = require('lodash'),
    async   = require('async'),
    flatten = require('flat'),
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

  self._app.get("/apos/salesforce/sync", function(req, res) {
    getConnection(function(err) {
      if (err) return console.log(err);
      var queries = {};

      var executeTasks = [],
          mapTasks = [],
          saveTasks = [],
          joinTasks = [];
      // Create each query object
      self.mappings.forEach(function(mapping) {
        mapping.Type = self._site.modules[mapping.aposObj];
        mapping.req = req; // getOne and putOne requires req object
        queries[mapping] = new Query(mapping);
        executeTasks.push(queries[mapping].execute);
        mapTasks.push(queries[mapping].map);
        saveTasks.push(queries[mapping].save);
        joinTasks.push(queries[mapping].join);
      });
      async.series({
          execute: function (callback) {
            async.parallel(executeTasks, function () {
              callback();
            });
          },
          map: function (callback) {
            async.parallel(mapTasks, function () {
              callback();
            });
          },
          save: function (callback) {
            async.parallel(saveTasks, function () {
              callback();
            });
          },
          join: function (callback) {
            async.parallel(joinTasks, function () {
              callback();
            });
          }
        }, 
        function() {
          console.log("done syncing");
        }
      );
    })

    res.redirect('/');
  });

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

  var Query = function (mapping) {
    var self = this;

    var Type = mapping.Type;
    var req = mapping.req;

    // Construct the SOQL Query
    var queryFields = [];
    var whereClauses = [];
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
      var sfJoin = mapping.joins[aposJoin].sfType;
      queryFields.push('(SELECT Entity.Id FROM ' + sfJoin + ' AS Entity)');
    }
    // Add custom WHERE clauses
    if (mapping.where) {
      whereClauses.push.apply(whereClauses, mapping.where)
    }
    // Create query string
    self.queryString = "SELECT Id, " + queryFields.join(', ') + 
                      " FROM " + mapping.sfObj + 
                      ((whereClauses.length > 0) ? " WHERE " + whereClauses.join(" AND ") : "") +
                      " LIMIT 5000";

    self.sfResults = [];
    self.aposResults = [];

    // Executes the constructed query
    self.execute = function(callback) {
      connection.query(self.queryString, function(err, result) {
        if (err) return callback(err);
        result.records.forEach(function(sfObj) {
          // To deal with accessing nested elements
          sfObj = flatten(sfObj, {safe: true});
          self.sfResults.push(sfObj);
        });
        callback();
      });
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
        function(aposObj, innerCallback) {
          Type.getOne(req, {sfId: aposObj.sfId}, {}, function(err, item) {
            if(err) return innerCallback(err);
            if(!item) {
              item = aposObj;
            } else {
              console.log(aposObj.sfId + " -+- " + item.title);
              _.extend(item, aposObj);
            }
            // Save the Apostrophe object
            Type.putOne(req, {}, item, function(err) {
              if(err) return innerCallback(err);
              innerCallback();
            });
          });
        }, function(err) {
          callback(err);
        }
      );
    }

    // Creates joins in the saved apostrophe objects
    self.join = function(callback) {
      self.sfResults.forEach(function(sfObj) {
        for(aposJoin in mapping.joins) {
          var join = mapping.joins[aposJoin];
          var joinResults = sfObj[join.sfType + '.records'];
          console.log(joinResults);
          for(i in joinResults) {
            options.site.modules[join.aposType].getOne(req, {sfId: joinResults[i].Id}, {}, function(err, item) {
              if(err) console.log(err);
              //if(joinResults[i]) console.log(joinResults[i].Id + "---" + (item ? item.title : "none"));
            });
          }
        }
      });
      callback();
    }
  }

  // Add admin UI elements
  self._apos.mixinModuleAssets(self, 'salesforce', __dirname, options);
  self._apos.addLocal('aposSalesforceMenu', function(args) {
    var result = self.render('menu', args);
    return result;
  });

  // Invoke the callback. This must happen on next tick or later!
  if (callback) {
    return process.nextTick(function() {
      return callback(null);
    });
  }
}

// Export the constructor so others can subclass
salesforce.Construct = Construct;
