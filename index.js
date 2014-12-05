var request = require('request'),
    jsforce = require('jsforce')
    flatten = require('flat');

module.exports = factory;

function factory(options, callback) {
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

  // Salesforce authentication stuffs
  self.sfUsername = options.sfUsername;
  self.sfPassword = options.sfPassword;

  self._app.get("/salesforce/refresh", function(req, res) {

    var conn = new jsforce.Connection({});

    conn.login(self.sfUsername, self.sfPassword, function(err, userInfo) {
      self.mappings.forEach(function(mapping) {
        // Get the relevant Apostrophe object for the mapping
        var Type = self._site.modules[mapping.aposObj];

        // Create a SOQL Query
        var queryFields = [];
        for(aposField in mapping.fields) {
          sfFields = mapping.fields[aposField];
          if(!(sfFields instanceof Array)) {
            sfFields = [sfFields];
          }
          queryFields.push.apply(queryFields, sfFields);
        }
        // Should add some configurable criteria, e.g. custom flag that allows records to be imported by Apostrophe
        var queryString = "SELECT Id, " + queryFields.join(', ') + " FROM " + mapping.sfObj + " LIMIT 100";

        // This call gets the data
        conn.query(queryString, function(err, result) {
          result.records.forEach(function(sfObj) {
            // To deal with addressing nested elements
            sfObj = flatten(sfObj);

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
            // Save the Apostrophe object
            Type.putOne(req, {}, aposObj, function(err) {
              if(err){
                console.error(err);
              }
              // saved from salesforce successfully!
            });
          });
        });
      });
    });

    
    res.send('done');
  });

  // Invoke the callback. This must happen on next tick or later!
  if (callback) {
    return process.nextTick(function() {
      return callback(null);
    });
  }
}

// Export the constructor so others can subclass
factory.Construct = Construct;
