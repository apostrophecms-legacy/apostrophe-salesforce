# apostrophe-salesforce

This is for Apostrophe 0.5. It has not yet been ported to 2.x. Feel free to port it.

Example Apostrophe config block:

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

Visit `/apos/salesforce/sync` to sync. (TODO: this is a single blocking GET request, which could fail depending on your proxy's timeouts. A scoreboard in aposCache should be used to enable a periodically refreshing status display and cancel functionality.)

If you add `?resync=1` to the URL it will resync everything, not just changes since the last sync. This is useful if you have changed your mappings.

## Progress display

There is a very basic progress display page. It refreshes every 5 seconds.

If the progress page seems empty to you, your nunjucks block names are probably not the same as ours. Just override the `progress.html` template of this module at project level.

## Command line

You can also sync at the command line:

```
node app apostrophe:salesforce [--resync]
```

## Changelog

0.1.2: `version: false` to prevent massive proliferation of versions in the database on every salesforce sync.

0.1.1: Resync option on the command line.

0.1.0: Progress display and resync option added. Async code cleaned up and parallelism reduced to levels that don't starve CPU and I/O for the site.
