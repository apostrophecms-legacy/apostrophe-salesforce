var request = require('request');

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
  self.sfClientId = options.sfClientId;
  self.sfSecret = options.sfSecret;
  self.sfUsername = options.sfUsername;
  self.sfPassword = options.sfPassword;

  self._app.get("/salesforce/refresh", function(req, res) {

    self.mappings.forEach(function(mapping) {
      // Get the relevant Apostrophe object for the mapping
      var Type = self._site.modules[mapping.aposObj];

      var auth = {
        grant_type: "password",
        client_id: self.sfClientId,
        client_secret: self.sfSecret,
        username: self.sfUsername,
        password: self.sfPassword
      }

      // First call gets a token
      request.post({url: 'https://login.salesforce.com/services/oauth2/token', form: auth}, function(err, resp, body) {
        var parsedBody = JSON.parse(body);
        var access_token = parsedBody.access_token;
        var instance_url = parsedBody.instance_url;

        // Create a SOQL Query
        var queryFields = [];
        for(aposField in mapping.fields) {
          sfFields = mapping.fields[aposField];
          if(!(sfFields instanceof Array)) {
            sfFields = [sfFields];
          }
          sfFields.forEach(function(sfField) {
            queryFields.push(sfField);
          });
        }
        // Should add some configurable criteria, e.g. custom flag that allows records to be imported by Apostrophe
        var queryString = encodeURI("SELECT Id, " + queryFields.join(', ') + " FROM " + mapping.sfObj + " WHERE Id='006C000000eKRT4' LIMIT 100");

        // This call gets the data
        request.get(instance_url + '/services/data/v26.0/query/?q=' + queryString, {'auth': {'bearer': access_token}}, function(err, resp, body) {
          var parsedBody = JSON.parse(body);
          parsedBody.records.forEach(function(sfObj) {
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
                // To account for nested objects
                var sfValue = sfObj;
                sfField.split('.').forEach(function(subField) {
                  if(typeof sfValue !== 'undefined' && sfValue !== null) {
                    sfValue = sfValue[subField];
                  } else {
                    sfValue = null;
                  }
                });
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