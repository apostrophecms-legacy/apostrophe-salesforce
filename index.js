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
    withConnection(function (err, connection) {
      if (err) callback(err);
      var queries = {};
      for (i in mappings) {
        var mapping = mappings[i];
        mapping.Type = self._site.modules[mapping.aposObj];
        queries[mapping] = new Query(mapping);
      });
      async.parallel()
    });
    res.redirect('/');
  });

  function withConnection (callback) {
    var connection = new jsforce.Connection({});
    connection.login(self.sfUsername, self.sfPassword, function(err, userInfo) {
      return callback(err, connection);
    });
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

function Query = function (mapping) {
  var self = this;

  var Type = mapping.Type;

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
    var sfJoin = mapping.joins[aposJoin];
    queryFields.push('(SELECT Entity.Id FROM ' + sfJoin + ' AS Entity)');
  }
  // Add custom WHERE clauses
  if (mapping.where) {
    whereFields.push.apply(whereFields, mapping.where)
  }
  // Create query string
  self.queryString = "SELECT Id, " + queryFields.join(', ') + 
                    " FROM " + mapping.sfObj + 
                    ((whereClauses.length > 0) ? " WHERE " + whereClauses.join(" AND ") : "") +
                    " LIMIT 1000";

  self.execute = function(callback) {
    // Execute the query
    conn.query(self.queryString, function(err, result) {
      if(err) callback(err);
      result.records.forEach(function(sfObj) {
        // To deal with addressing nested elements
        sfObj = flatten(sfObj, {safe: true});

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
        }
        for(aposJoin in mapping.joins) {
          var sfJoin = mapping.joins[aposJoin];
          var joinResults = sfObj[sfJoin + '.records'];
          for(i in joinResults) {
            console.log(joinResults[i].Id);

          }
        }

        // De-duping step
        Type.getOne(req, {sfId: aposObj.sfId}, {}, function(err, item) {
          if(err) {
            console.error(err);
          }
          if(!item) {
            item = aposObj;
          } else {
            _.extend(item, aposObj);
          }
          // Save the Apostrophe object
          Type.putOne(req, {}, item, function(err) {
            if(err) {
              console.error(err);
            }
            // saved from salesforce successfully!
          });
        });
      });
    });
  }

  self.save = function(callback) {

  }

  self.join = function(callback) {

  }
}

// Export the constructor so others can subclass
salesforce.Construct = Construct;
