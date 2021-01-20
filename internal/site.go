package internal

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// ProcessSite will update a given site's counts based on the story documents
// that compose the values for that.
func ProcessSite(ctx context.Context, db *mongo.Database, tenantID, siteID string) error {
	// Create the $match clause that will limit the documents processed.
	match := bson.D{
		primitive.E{Key: "tenantID", Value: tenantID},
		primitive.E{Key: "siteID", Value: siteID},
	}

	// Create the $group clause that will collect all the parts of data from
	// stories that are important to the count update.
	// TODO: implement
	group := bson.D{}

	// Start aggregating.
	cursor, err := db.Collection("stories").Aggregate(ctx, mongo.Pipeline{
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

	// While there is still results to handle, decode the results.
	for cursor.Next(ctx) {
		// TODO: decode the result
	}

	if err := cursor.Err(); err != nil {
		return errors.Wrap(err, "could not iterate on aggregation cursor")
	}

	return nil
}
