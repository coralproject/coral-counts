package main

import (
	"context"
	"coral-counts/counts"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func run(c *cli.Context) error {
	// Grab the parameters from the flags.
	tenantID := c.String("tenantID")
	siteID := c.String("siteID")
	databaseURI := c.String("mongoDBURI")
	dryRun := c.Bool("dryRun")
	disableWatcher := c.Bool("disableWatcher")
	mongoDBConnectTimeout := c.Duration("mongoDBConnectTimeout")

	// Set the batch size.
	counts.MaxBatchWriteSize = c.Int("batchSize")

	// Parse the database name out of the path component of the uri.
	u, err := url.Parse(databaseURI)
	if err != nil {
		return errors.Wrap(err, "can not parse the --mongoDBURI")
	}
	if len(u.Path) < 2 {
		return errors.Errorf("expected database name in path component of --mongoDBURI, found %s", u.Path)
	}
	databaseName := u.Path[1:]

	// Create a context for connecting to MongoDB.
	ctx, cancel := context.WithTimeout(context.Background(), mongoDBConnectTimeout)
	defer cancel()

	// Connect to MongoDB now.
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(databaseURI))
	if err != nil {
		return errors.Wrap(err, "cannot connect to mongo")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	// Ensure we're connected to the primary.
	ctx, cancel = context.WithTimeout(context.Background(), mongoDBConnectTimeout)
	defer cancel()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return errors.Wrap(err, "cannot ping mongo")
	}

	// Get the database handle for the database we're connecting to.
	db := client.Database(databaseName)

	// Create the watcher, and start it.
	watcher := counts.NewWatcher(db, tenantID, siteID)

	if !disableWatcher {
		logrus.Info("starting watcher")

		// Start monitoring for updates to the comments collection to ensure that we
		// can tag any stories/sites that might have gotten dirty since we started.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			// Cancel the context also if the watcher exits early.
			defer cancel()

			if err := watcher.Watch(ctx); err != nil {
				logrus.WithError(err).Warn("watcher has failed to start")
			}
		}()

		// Wait for the changestream to start.
		if err := watcher.Wait(ctx); err != nil {
			logrus.WithError(err).Fatal("could not wait for watcher to start")
		}
	} else {
		logrus.Warn("not starting watcher, --disableWatcher was used")
	}

	started := time.Now()

	// The watcher will collect an event for every comment that is inserted or
	// updated since it started watching. We'll use this to trigger targeted
	// re-runs of the recomputation to help ensure that we've scanned everything.

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Process the stories.
	if err := counts.ProcessStories(ctx, db, tenantID, siteID, nil, dryRun); err != nil {
		return errors.Wrap(err, "could not process stories")
	}

	// Process the site.
	if err := counts.ProcessSite(ctx, db, tenantID, siteID, dryRun); err != nil {
		return errors.Wrap(err, "could not process site")
	}

	// Process the users.
	if err := counts.ProcessUsers(ctx, db, tenantID, siteID, nil, dryRun); err != nil {
		return errors.Wrap(err, "could not process users")
	}

	for {
		// Get all the dirty story ID's from the watcher. This will also flush these
		// events from the watcher.
		dirty := watcher.Dirty()
		if dirty == nil {
			logrus.Info("no dirty stories or users were found")
			break
		}

		logrus.WithFields(logrus.Fields{
			"stories": len(dirty.StoryIDs),
			"users":   len(dirty.UserIDs),
		}).Info("recalculating dirty documents")

		// Process the dirty stories.
		if len(dirty.StoryIDs) > 0 {
			if err := counts.ProcessStories(ctx, db, tenantID, siteID, dirty.StoryIDs, dryRun); err != nil {
				return errors.Wrap(err, "could not process dirty stories")
			}

			// Process the site.
			if err := counts.ProcessSite(ctx, db, tenantID, siteID, dryRun); err != nil {
				return errors.Wrap(err, "could not process dirty site")
			}
		}

		// Process the dirty users.
		if len(dirty.UserIDs) > 0 {
			if err := counts.ProcessUsers(ctx, db, tenantID, siteID, dirty.UserIDs, dryRun); err != nil {
				return errors.Wrap(err, "could not process users")
			}
		}
	}

	logrus.WithField("took", time.Since(started).String()).Info("finished processing")

	return nil
}

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	app := cli.NewApp()
	app.Name = "coral-counts"
	app.Usage = "a tool to update comment counts after a import"
	app.Version = fmt.Sprintf("%v, commit %v, built at %v", version, commit, date)
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "tenantID",
			Usage:    "ID for the Tenant we're refreshing counts on",
			Required: true,
			EnvVars:  []string{"TENANT_ID"},
		},
		&cli.StringFlag{
			Name:     "siteID",
			Usage:    "ID for the Site we're refreshing counts on",
			Required: true,
			EnvVars:  []string{"SITE_ID"},
		},
		&cli.StringFlag{
			Name:     "mongoDBURI",
			Usage:    "URI for the MongoDB instance that we're refreshing counts on",
			Required: true,
			EnvVars:  []string{"MONGODB_URI"},
		},
		&cli.BoolFlag{
			Name:    "dryRun",
			Usage:   "when used, this tool will not write any data to the database",
			EnvVars: []string{"DRY_RUN"},
		},
		&cli.BoolFlag{
			Name:    "disableWatcher",
			Usage:   "when used, this tool will not attempt to watch for changes to prevent races",
			EnvVars: []string{"DISABLE_WATCHER"},
		},
		&cli.IntFlag{
			Name:    "batchSize",
			Usage:   "specify the batch size to write the update for the stories",
			Value:   1000,
			EnvVars: []string{"BATCH_SIZE"},
		},
		&cli.DurationFlag{
			Name:    "mongoDBConnectTimeout",
			Usage:   "used to specify the timeout for connecting to MongoDB",
			Value:   1 * time.Minute,
			EnvVars: []string{"MONGODB_CONNECT_TIMEOUT"},
		},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.WithError(err).Fatal()
	}
}
