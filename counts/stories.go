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

type StoryCommentCounts struct {
	Action          CommentActionCounts    `bson:"action"`
	Status          CommentStatusCounts    `bson:"status"`
	ModerationQueue CommentModerationQueue `bson:"moderationQueue"`
}

func (scc *StoryCommentCounts) Merge(counts *StoryCommentCounts) {
	// Action
	for key, count := range counts.Action {
		scc.Action[key] += count
	}

	// Status
	scc.Status.Approved += counts.Status.Approved
	scc.Status.None += counts.Status.None
	scc.Status.Premod += counts.Status.Premod
	scc.Status.Rejected += counts.Status.Rejected
	scc.Status.SystemWithheld += counts.Status.SystemWithheld

	// ModerationQueue
	scc.ModerationQueue.Total += counts.ModerationQueue.Total
	scc.ModerationQueue.Queues.Unmoderated += counts.ModerationQueue.Queues.Unmoderated
	scc.ModerationQueue.Queues.Reported += counts.ModerationQueue.Queues.Reported
	scc.ModerationQueue.Queues.Pending += counts.ModerationQueue.Queues.Pending
}

// Story is a Story in Coral.
type Story struct {
	ID            string             `bson:"id"`
	CommentCounts StoryCommentCounts `bson:"commentCounts"`
}

// Increment will increment the comment counts based on the passed comment.
func (s *Story) Increment(comment *Comment) {
	// Action
	s.CommentCounts.Action.Increment(comment)

	// Status
	s.CommentCounts.Status.Increment(comment)

	// ModerationQueue
	s.CommentCounts.ModerationQueue.Increment(comment)
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

	started := time.Now()
	logrus.WithField("siteID", siteID).Info("loading stories from comments")

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

			story.CommentCounts.Action = make(map[string]int)
		}

		// Increment the story document based on this comment.
		story.Increment(&comment)
	}

	if err := cursor.Err(); err != nil {
		return errors.Wrap(err, "could not iterate on cursor")
	}

	logrus.WithFields(logrus.Fields{
		"stories": len(stories),
		"took":    time.Since(started),
	}).Info("loaded stories from comments")

	// We will collect all the bulk write operations that we'll use to update the
	// stories here.
	updates := make([]mongo.WriteModel, 0)

	// Iterate over the stories in the map.
	for storyID, story := range stories {
		// Create the new update.
		update := mongo.NewUpdateOneModel()

		// Select the story we're updating.
		update.SetFilter(bson.D{
			primitive.E{Key: "tenantID", Value: tenantID},
			primitive.E{Key: "siteID", Value: siteID},
			primitive.E{Key: "id", Value: storyID},
		})

		// Update it with the counts.
		update.SetUpdate(bson.D{
			primitive.E{Key: "$set", Value: bson.D{
				primitive.E{Key: "commentCounts", Value: story.CommentCounts},
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
	}

	return nil
}
