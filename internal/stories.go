package internal

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

// MaxBatchWriteSize is the maximum size of batch write operations.
const MaxBatchWriteSize = 200

// ProcessStories will iterate over each stories comments and aggregate the
// results to update the cached counts for each story. `storyID`'s are optional,
// and will limit the total stories that are processed.
func ProcessStories(ctx context.Context, db *mongo.Database, tenantID, siteID string, storyIDs []string) error {
	// Create the $match clause that will limit the documents processed.
	match := bson.D{
		primitive.E{Key: "tenantID", Value: tenantID},
		primitive.E{Key: "siteID", Value: siteID},
	}

	// If storyID's are specified (and contains id's), then we should limit this
	// aggregation to only those comments that are from those stories.
	if len(storyIDs) > 0 {
		match = append(match, primitive.E{
			Key: "storyID",
			Value: bson.D{
				primitive.E{
					Key:   "$in",
					Value: storyIDs,
				},
			},
		})
	}

	// Create the $group clause that will collect all the parts of data from
	// comments that are important to the count update.
	// TODO: implement
	group := bson.D{}

	// Start aggregating.
	cursor, err := db.Collection("comments").Aggregate(ctx, mongo.Pipeline{
		match,
		group,
	})
	if err != nil {
		return errors.Wrap(err, "could not create the aggregation cursor")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		if err := cursor.Close(ctx); err != nil {
			panic(err)
		}
	}()

	// We will collect all the bulk write operations that we'll use to update the
	// stories here.
	updates := make([]mongo.WriteModel, 0)

	// While there is still results to handle, decode the results.
	for cursor.Next(ctx) {
		// TODO: decode the result and add to the bulk write operation.

		// If we have more updates than the max size, then process them now.
		if len(updates) >= MaxBatchWriteSize {
			res, err := db.Collection("stories").BulkWrite(ctx, updates, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return errors.Wrap(err, "could not bulk write story updates")
			}

			logrus.WithFields(logrus.Fields{
				"updates":  len(updates),
				"modified": res.ModifiedCount,
			}).Info("wrote bulk story updates")

			// Reset the updates slice.
			updates = make([]mongo.WriteModel, 0)
		}
	}

	if err := cursor.Err(); err != nil {
		return errors.Wrap(err, "could not iterate on aggregation cursor")
	}

	// If we have updates leftover, process them now.
	if len(updates) > 0 {
		res, err := db.Collection("stories").BulkWrite(ctx, updates, options.BulkWrite().SetOrdered(false))
		if err != nil {
			return errors.Wrap(err, "could not bulk write story updates")
		}

		logrus.WithFields(logrus.Fields{
			"updates":  len(updates),
			"modified": res.ModifiedCount,
		}).Info("wrote bulk story updates")

		// Reset the updates slice.
		updates = make([]mongo.WriteModel, 0)
	}

	return nil
}
