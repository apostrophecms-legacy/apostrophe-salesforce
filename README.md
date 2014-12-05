# apostrophe-salesforce
Still in the oven

Example Apostrophe config block.

    'apostrophe-salesforce': {
      sfUsername: "SalesforceUsername",
      sfPassword: "SalesforcePassword",
      mappings: [
        {
          sfObj: 'sfObject',
          aposObj: 'aposObject',
          fields: {
            aposFieldName: 'sfFieldName',
            aposConcatFieldName: ['sfField1', 'sfField2', 'sfField3'],
            aposNestFieldName: 'sfJoinField.sfField'
          }
        }
      ]
    }
