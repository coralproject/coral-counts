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

// Comment is a Comment in Coral.
type Comment struct {
	StoryID      string           `bson:"storyID"`
	Status       string           `bson:"status"`
	ActionCounts map[string]int64 `bson:"actionCounts"`
}

// Story is a Story in Coral.
type Story struct {
	ID            string        `bson:"id"`
	CommentCounts CommentCounts `bson:"commentCounts"`
}

// Increment will increment the comment counts based on the passed comment.
func (s *Story) Increment(comment *Comment) {
	// Action
	for key, count := range comment.ActionCounts {
		s.CommentCounts.Action[key] += count
	}

	// Status
	switch comment.Status {
	case "APPROVED":
		s.CommentCounts.Status.Approved++
	case "NONE":
		s.CommentCounts.Status.None++
	case "PREMOD":
		s.CommentCounts.Status.Premod++
	case "REJECTED":
		s.CommentCounts.Status.Rejected++
	case "SYSTEM_WITHHELD":
		s.CommentCounts.Status.SystemWithheld++
	}

	// ModerationQueue
	switch comment.Status {
	case "NONE":
		s.CommentCounts.ModerationQueue.Total++
		s.CommentCounts.ModerationQueue.Queues.Unmoderated++

		// If this comment has a flag on it, then it should also be in the reported
		// queue.
		if count, ok := comment.ActionCounts["FLAG"]; ok && count > 0 {
			s.CommentCounts.ModerationQueue.Queues.Reported++
		}
	case "PREMOD":
		s.CommentCounts.ModerationQueue.Total++
		s.CommentCounts.ModerationQueue.Queues.Unmoderated++
		s.CommentCounts.ModerationQueue.Queues.Pending++
	case "SYSTEM_WITHHELD":
		s.CommentCounts.ModerationQueue.Total++
		s.CommentCounts.ModerationQueue.Queues.Unmoderated++
		s.CommentCounts.ModerationQueue.Queues.Pending++
	}
}

// ProcessStories will iterate over each stories comments and aggregate the
// results to update the cached counts for each story. `storyID`'s are optional,
// and will limit the total stories that are processed.
func ProcessStories(ctx context.Context, db *mongo.Database, tenantID, siteID string, storyIDs []string, dryRun bool) error {
	// Create the filter that will limit the documents processed.
	filter := bson.D{
		primitive.E{Key: "tenantID", Value: tenantID},
		primitive.E{Key: "siteID", Value: siteID},
	}

	// If storyID's are specified (and contains id's), then we should limit this
	// query to only those comments that are from those stories.
	if len(storyIDs) > 0 {
		filter = append(filter, primitive.E{
			Key: "storyID",
			Value: bson.D{
				primitive.E{
					Key:   "$in",
					Value: storyIDs,
				},
			},
		})
	}

	// Configure the projection to only get fields we care about.
	projection := bson.D{
		primitive.E{Key: "storyID", Value: 1},
		primitive.E{Key: "status", Value: 1},
		primitive.E{Key: "actionCounts", Value: 1},
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

	// Store all the stories in this map.
	stories := make(map[string]*Story)

	// While there is still results to handle, decode the results.
	for cursor.Next(ctx) {
		var comment Comment
		if err := cursor.Decode(&comment); err != nil {
			return errors.Wrap(err, "could not decode result")
		}

		// Create the story in the map if it isn't already.
		story, ok := stories[comment.StoryID]
		if !ok {
			story = &Story{}
			stories[comment.StoryID] = story

			story.CommentCounts.Action = make(map[string]int64)
		}

		// Increment the story document based on this comment.
		story.Increment(&comment)
	}

	if err := cursor.Err(); err != nil {
		return errors.Wrap(err, "could not iterate on cursor")
	}

	logrus.WithFields(logrus.Fields{
		"stories": len(stories),
	}).Info("finished loading stories")

	// We will collect all the bulk write operations that we'll use to update the
	// stories here.
	updates := make([]mongo.WriteModel, 0)

	// Iterate over the stories in the map.
	for storyID, story := range stories {
		// Create the new update.
		update := mongo.NewUpdateOneModel()

		// Select the story we're updating.
		update.SetFilter(bson.D{
			primitive.E{Key: "id", Value: storyID},
			primitive.E{Key: "siteID", Value: siteID},
		})

		// Update it with the counts.
		update.SetUpdate(bson.D{
			primitive.E{Key: "$set", Value: bson.D{
				primitive.E{Key: "commentCounts", Value: story.CommentCounts},
			}},
		})

		// Add the new update model.
		updates = append(updates, update)

		// If we have more updates than the max size, then process them now.
		if len(updates) >= MaxBatchWriteSize {
			if dryRun {
				logrus.WithFields(logrus.Fields{
					"updates": len(updates),
				}).Info("not writing bulk story updates as --dryRun is enabled")

				// Reset the updates slice.
				updates = make([]mongo.WriteModel, 0)

				continue
			}

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

	// If we have updates leftover, process them now.
	if len(updates) > 0 {
		if dryRun {
			logrus.WithFields(logrus.Fields{
				"updates": len(updates),
			}).Info("not writing bulk story updates as --dryRun is enabled")

			// Reset the updates slice.
			updates = make([]mongo.WriteModel, 0)

			return nil
		}

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
