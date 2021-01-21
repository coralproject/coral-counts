package internal

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// NewWatcher will return a watcher that can watch for collection changes to
// ensure we're in sync.
func NewWatcher(db *mongo.Database, tenantID, siteID string) *Watcher {
	return &Watcher{
		db:       db,
		tenantID: tenantID,
		siteID:   siteID,
		events:   make([]WatchEvent, 0),
	}
}

// WatchEvent is used to return which record has been modified.
type WatchEvent struct {
	OperationType string `bson:"opeartionType"`
	FullDocument  struct {
		ID      string `bson:"id"`
		StoryID string `bson:"storyID"`
	} `bson:"fullDocument"`
}

// Watcher can be used to monitor for dirty stories/sites to trigger future
// update operations.
type Watcher struct {
	db       *mongo.Database
	tenantID string
	siteID   string
	events   []WatchEvent
	mux      sync.Mutex
}

// Watch will watch for changes to the comments collection, and mark those
// stories/sites as dirty so that we can re-run on changes.
func (w *Watcher) Watch(ctx context.Context) error {
	// Create the change stream that we'll use to monitor the collection for any
	// insertions or updates to any comments on the specified tenant.
	cs, err := w.db.Collection("comments").Watch(ctx, mongo.Pipeline{
		bson.D{
			primitive.E{
				Key: "$match",
				Value: bson.D{
					primitive.E{
						Key: "operationType",
						Value: bson.D{
							primitive.E{
								Key:   "$in",
								Value: []string{"insert", "update"},
							},
						},
					},
					primitive.E{
						Key:   "fullDocument.tenantID",
						Value: w.tenantID,
					},
					primitive.E{
						Key:   "fullDocument.siteID",
						Value: w.siteID,
					},
				},
			},
		},
	}, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		return errors.Wrap(err, "could not watch the change stream")
	}
	defer cs.Close(ctx)

	// Continue iterating over this change stream until either the context is
	// cancelled or there is an error.
	for cs.Next(ctx) {
		var event WatchEvent
		if err := cs.Decode(&event); err != nil {
			return errors.Wrap(err, "could not decode change stream event")
		}

		logrus.WithFields(logrus.Fields{
			"commentID":     event.FullDocument.ID,
			"storyID":       event.FullDocument.StoryID,
			"opeartionType": event.OperationType,
		}).Info("a comment has been changed, marking it's story as dirty")

		// Add this record.
		w.mux.Lock()
		w.events = append(w.events, event)
		w.mux.Unlock()
	}

	if err := cs.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}

		return errors.Wrap(err, "an error occurred while processing the change stream")
	}

	return nil
}

// Dirty will return a list of all the story id's that are dirty.
func (w *Watcher) Dirty() []string {
	// Lock access to the records, as we'll be trying to get them all.
	w.mux.Lock()
	defer w.mux.Unlock()

	// If we have no records, then return nothing!
	if len(w.events) == 0 {
		return nil
	}

	// Deduplicate all the story id's.
	storyIDMap := make(map[string]struct{})
	for _, event := range w.events {
		// If we've already seen this one before, don't add it.
		if _, ok := storyIDMap[event.FullDocument.StoryID]; ok {
			continue
		}

		// This is new, add it!
		storyIDMap[event.FullDocument.StoryID] = struct{}{}
	}

	// Turn the map into a slice.
	storyIDs := make([]string, 0, len(storyIDMap))
	for storyID := range storyIDMap {
		storyIDs = append(storyIDs, storyID)
	}

	// Reset the underlying slice.
	w.events = make([]WatchEvent, 0)

	return storyIDs
}
