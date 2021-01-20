package main

import (
	"context"
	"coral-counts/internal"
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
	databaseURI := c.String("mongoURI")

	// Parse the database name out of the path component of the uri.
	u, err := url.Parse(databaseURI)
	if err != nil {
		return errors.Wrap(err, "can not parse the --mongoURI")
	}
	if len(u.Path) < 2 {
		return errors.Errorf("expected database name in path component of --mongoURI, found %s", u.Path)
	}
	databaseName := u.Path[1:]

	// Create a context for connecting to MongoDB.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return errors.Wrap(err, "cannot ping mongo")
	}

	// Get the database handle for the database we're connecting to.
	db := client.Database(databaseName)

	// Start monitoring for updates to the comments collection to ensure that we
	// can tag any stories/sites that might have gotten dirty since we started.
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	// Create the watcher, and start it.
	watcher := internal.NewWatcher(db, tenantID, siteID)
	go func() {
		if err := watcher.Watch(ctx); err != nil {
			logrus.WithError(err).Fatal("could not watch for changes")
		}
	}()

	// The watcher will collect an event for every comment that is inserted or
	// updated since it started watching. We'll use this to trigger targeted
	// re-runs of the recomputation to help ensure that we've scanned everything.

	// Process the stories.
	if err := internal.ProcessStories(ctx, db, tenantID, siteID, nil); err != nil {
		return errors.Wrap(err, "could not process stories")
	}

	// Process the site.
	if err := internal.ProcessSite(ctx, db, tenantID, siteID); err != nil {
		return errors.Wrap(err, "could not process site")
	}

	for {
		// Get all the dirty story ID's from the watcher. This will also flush these
		// events from the watcher.
		storyIDs := watcher.Dirty()
		if len(storyIDs) == 0 {
			logrus.Info("no more dirty stories were found")
			break
		}

		// Process the dirty stories.
		if err := internal.ProcessStories(ctx, db, tenantID, siteID, storyIDs); err != nil {
			return errors.Wrap(err, "could not process dirty stories")
		}

		// Process the site.
		if err := internal.ProcessSite(ctx, db, tenantID, siteID); err != nil {
			return errors.Wrap(err, "could not process dirty site")
		}
	}

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
	app.Version = fmt.Sprintf("%v, commit %v, built at %v", version, commit, date)
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:     "tenantID",
			Usage:    "ID for the Tenant we're refreshing counts on",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "siteID",
			Usage:    "ID for the Site we're refreshing counts on",
			Required: true,
		},
		&cli.StringFlag{
			Name:     "mongoURI",
			Usage:    "URI for the MongoDB instance that we're refreshing counts on",
			Required: true,
		},
	}
	app.Action = run

	if err := app.Run(os.Args); err != nil {
		logrus.WithError(err).Fatal()
	}
}
