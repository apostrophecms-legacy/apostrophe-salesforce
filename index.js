var _ = require('lodash'),
    flatten = require('flat'),
    jsforce = require('jsforce');

module.exports = salesforce;

function salesforce(options, callback) {
  return new Construct(options, callback);
}

function Construct(options, callback) {
  var self = this;
  // Add a bunch of methods to self here, then...
  self._app = options.app;
  self._apos = options.apos;
  self._site = options.site;

  // The configuration of how Salesforce data maps into Apostrophe
  self.mappings = options.mappings;

  self._apos.mixinModuleAssets(self, 'salesforce', __dirname, options);

  // Salesforce authentication stuffs
  self.sfUsername = options.sfUsername;
  self.sfPassword = options.sfPassword;

  self._app.get("/salesforce/sync", function(req, res) {

    var conn = new jsforce.Connection({});

    conn.login(self.sfUsername, self.sfPassword, function(err, userInfo) {
      if(err) console.log(err);
      self.mappings.forEach(function(mapping) {
        // Get the relevant Apostrophe object for the mapping
        var Type = self._site.modules[mapping.aposObj];

        // Build SOQL Query
        var queryFields = [];
        var whereClauses = [];
        for(aposField in mapping.fields) {
          sfFields = mapping.fields[aposField];
          if(!(sfFields instanceof Array)) {
            sfFields = [sfFields];
          }
          queryFields.push.apply(queryFields, sfFields);

          // Add required field stipulations
          if(mapping.required && mapping.required.indexOf(aposField) >= 0) {
            sfFields.forEach(function(sfField) {
              whereClauses.push(sfField + " != null")
            });
          }
        }
        if(mapping.where) {
          mapping.where.forEach(function(whereClause) {
            whereClauses.push(whereClause);
          });
        }
        // Should add some configurable criteria, e.g. custom flag that allows records to be imported by Apostrophe
        var queryString = "SELECT Id, " + queryFields.join(', ') + 
                          " FROM " + mapping.sfObj + 
                          ((whereClauses.length > 0) ? " WHERE " + whereClauses.join(" AND ") : "")
                          + " LIMIT 1000";
        //console.log(queryString);

        // Execute query
        conn.query(queryString, function(err, result) {
          if (err) console.log(err);
          result.records.forEach(function(sfObj) {
            // To deal with addressing nested elements
            sfObj = flatten(sfObj, {safe: true});

            // Create new instance of object and associate with Salesforce id
            var aposObj = Type.newInstance();
            aposObj.sfId = sfObj.Id;

            // Add fields to Apostrophe object according to mapping configuration
            for (aposField in mapping.fields) {
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
      });
    });

    res.redirect('/');
  });

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
