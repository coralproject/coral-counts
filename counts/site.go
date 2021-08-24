package counts

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Site struct {
	ID            string             `bson:"id"`
	CommentCounts StoryCommentCounts `bson:"commentCounts"`
}

// ProcessSite will update a given site's counts based on the story documents
// that compose the values for that.
func ProcessSite(ctx context.Context, db *mongo.Database, tenantID, siteID string, dryRun bool) error {
	// Create the filter that will limit the documents processed.
	filter := bson.D{
		primitive.E{Key: "tenantID", Value: tenantID},
		primitive.E{Key: "siteID", Value: siteID},
	}

	// Configure the projection to only get fields we care about.
	projection := bson.D{
		primitive.E{Key: "id", Value: 1},
		primitive.E{Key: "commentCounts", Value: 1},
	}

	// Start querying.
	cursor, err := db.Collection("stories").Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return errors.Wrap(err, "could not create the cursor")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := cursor.Close(ctx); err != nil {
			panic(err)
		}
	}()

	// Store all the counts for this site.
	var site Site
	site.CommentCounts.Action = make(map[string]int)

	started := time.Now()
	logrus.Info("loading counts from site stories")

	// While there is still results to handle, decode the results.
	for cursor.Next(ctx) {
		var story Story
		if err := cursor.Decode(&story); err != nil {
			return errors.Wrap(err, "could not decode result")
		}

		// Increment the site document based on this story.
		site.CommentCounts.Merge(&story.CommentCounts)
	}

	if err := cursor.Err(); err != nil {
		return errors.Wrap(err, "could not iterate on cursor")
	}

	logrus.WithField("took", time.Since(started)).Info("loaded counts from site stories")

	if dryRun {
		logrus.WithFields(logrus.Fields{
			"commentCounts": site.CommentCounts,
		}).Info("not writing site update as --dryRun is enabled")
	} else {

		started = time.Now()
		logrus.Info("updating site")

		// Update the site.
		if _, err := db.Collection("sites").UpdateOne(ctx, bson.D{
			primitive.E{Key: "tenantID", Value: tenantID},
			primitive.E{Key: "id", Value: siteID},
		}, bson.D{
			primitive.E{Key: "$set", Value: bson.D{
				primitive.E{Key: "commentCounts", Value: site.CommentCounts},
			}},
		}); err != nil {
			return errors.Wrap(err, "could not update the site")
		}

		logrus.WithFields(logrus.Fields{
			"id":   siteID,
			"took": time.Since(started),
		}).Info("site updated")

	}

	return nil
}
