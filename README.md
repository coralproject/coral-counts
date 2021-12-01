# coral-counts

A command-line tool to update Coral comment counts after importing or migrating data.

![Test](https://github.com/coralproject/coral-counts/workflows/Test/badge.svg)

Visit the [Releases](https://github.com/coralproject/coral-counts/releases) page
to download a release of the `coral-counts` tool.

## Usage

```
NAME:
   coral-counts - a tool to update comment counts after a import

USAGE:
   coral-counts [global options] command [command options] [arguments...]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --tenantID value    ID for the Tenant we're refreshing counts on [$TENANT_ID]
   --siteID value      ID for the Site we're refreshing counts on [$SITE_ID]
   --mongoDBURI value  URI for the MongoDB instance that we're refreshing counts on [$MONGODB_URI]
   --dryRun            when used, this tool will not write any data to the database (default: false) [$DRY_RUN]
   --disableWatcher    when used, this tool will not attempt to watch for changes to prevent races (default: false) [$DISABLE_WATCHER]
   --batchSize value   specify the batch size to write the update for the stories (default: 1000) [$BATCH_SIZE]
   --help, -h          show help (default: false)
   --version, -v       print the version (default: false)
```
