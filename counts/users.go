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

type UserCommentCounts struct {
	Status CommentStatusCounts `bson:"status"`
}

type User struct {
	CommentCounts UserCommentCounts `bson:"commentCounts"`
}

func (u *User) Increment(comment *Comment) {
	u.CommentCounts.Status.Increment(comment)
}

func ProcessUsers(ctx context.Context, db *mongo.Database, tenantID, siteID string, authorIDs []string, dryRun bool) error {
	// Create the filter that will limit the documents processed.
	filter := bson.D{
		primitive.E{Key: "tenantID", Value: tenantID},
		primitive.E{Key: "siteID", Value: siteID},
	}

	// If storyID's are specified (and contains id's), then we should limit this
	// query to only those comments that are from those users.
	if len(authorIDs) > 0 {
		filter = append(filter, primitive.E{
			Key: "authorID",
			Value: bson.D{
				primitive.E{
					Key:   "$in",
					Value: authorIDs,
				},
			},
		})
	}

	// Configure the projection to only get fields we care about.
	projection := bson.D{
		primitive.E{Key: "authorID", Value: 1},
		primitive.E{Key: "status", Value: 1},
	}

	// Start querying.
	cursor, err := db.Collection("comments").Find(ctx, filter, options.Find().SetProjection(projection))
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

	// Store all the users in this map.
	users := make(map[string]*User)

	started := time.Now()
	logrus.WithField("siteID", siteID).Info("loading users from comments")

	// While there is still results to handle, decode the results.
	for cursor.Next(ctx) {
		var comment Comment
		if err := cursor.Decode(&comment); err != nil {
			return errors.Wrap(err, "could not decode result")
		}

		// Create the user in the map if it isn't already.
		user, ok := users[comment.AuthorID]
		if !ok {
			user = &User{}
			users[comment.AuthorID] = user
		}

		// Increment the user document based on this comment.
		user.Increment(&comment)
	}

	logrus.WithFields(logrus.Fields{
		"users": len(users),
		"took":  time.Since(started),
	}).Info("loaded users from comments")

	// We will collect all the bulk write operations that we'll use to update the
	// users here.
	updates := make([]mongo.WriteModel, 0)

	// Iterate over the users in the map.
	for userID, user := range users {
		// Create the new update.
		update := mongo.NewUpdateOneModel()

		// Select the story we're updating.
		update.SetFilter(bson.D{
			primitive.E{Key: "tenantID", Value: tenantID},
			primitive.E{Key: "id", Value: userID},
		})

		// Update it with the counts.
		update.SetUpdate(bson.D{
			primitive.E{Key: "$set", Value: bson.D{
				primitive.E{Key: "commentCounts", Value: user.CommentCounts},
			}},
		})

		update.SetHint(bson.D{
			primitive.E{Key: "tenantID", Value: 1},
			primitive.E{Key: "id", Value: 1},
		})

		// Add the new update model.
		updates = append(updates, update)

		// If we have more updates than the max size, then process them now.
		if len(updates) >= MaxBatchWriteSize {
			if dryRun {
				logrus.WithFields(logrus.Fields{
					"updates": len(updates),
				}).Info("not writing bulk user updates as --dryRun is enabled")

				// Reset the updates slice.
				updates = make([]mongo.WriteModel, 0)

				continue
			}

			res, err := db.Collection("users").BulkWrite(ctx, updates, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return errors.Wrap(err, "could not bulk write user updates")
			}

			logrus.WithFields(logrus.Fields{
				"updates":  len(updates),
				"modified": res.ModifiedCount,
			}).Info("wrote bulk user updates")

			// Reset the updates slice.
			updates = make([]mongo.WriteModel, 0)
		}
	}

	// If we have updates leftover, process them now.
	if len(updates) > 0 {
		if dryRun {
			logrus.WithFields(logrus.Fields{
				"updates": len(updates),
			}).Info("not writing bulk story updates as --dryRun is enabled")

			return nil
		}

		res, err := db.Collection("users").BulkWrite(ctx, updates, options.BulkWrite().SetOrdered(false))
		if err != nil {
			return errors.Wrap(err, "could not bulk write user updates")
		}

		logrus.WithFields(logrus.Fields{
			"updates":  len(updates),
			"modified": res.ModifiedCount,
		}).Info("wrote bulk user updates")
	}

	return nil
}
