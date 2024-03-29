package counts

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
	events := make([]WatchEvent, 0)

	return &Watcher{
		db:       db,
		tenantID: tenantID,
		siteID:   siteID,
		events:   events,
		ready:    make(chan struct{}),
	}
}

// WatchEvent is used to return which record has been modified.
type WatchEvent struct {
	OperationType string `bson:"operationType"`
	FullDocument  struct {
		ID       string `bson:"id"`
		AuthorID string `bson:"authorID"`
		StoryID  string `bson:"storyID"`
	} `bson:"fullDocument"`
}

// Watcher can be used to monitor for dirty stories/sites to trigger future
// update operations.
type Watcher struct {
	db       *mongo.Database
	tenantID string
	siteID   string
	events   []WatchEvent
	ready    chan struct{}
	mux      sync.Mutex
}

// Wait will wait until the watcher is listening for events or the context
// expires.
func (w *Watcher) Wait(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.ready:
			return nil
		}
	}
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

	// We're listening to events, send the ready signal!
	w.ready <- struct{}{}

	// Continue iterating over this change stream until either the context is
	// canceled or there is an error.
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

type DirtyKeys struct {
	StoryIDs []string
	UserIDs  []string
}

// Dirty will return a list of all the story id's that are dirty.
func (w *Watcher) Dirty() *DirtyKeys {
	// Lock access to the records, as we'll be trying to get them all.
	w.mux.Lock()
	defer w.mux.Unlock()

	// If we have no records, then return nothing!
	if len(w.events) == 0 {
		return nil
	}

	dirty := DirtyKeys{}

	// Deduplicate all the story and user id's.
	storyIDMap := make(map[string]struct{})
	userIDMap := make(map[string]struct{})
	for _, event := range w.events {
		// If we've already seen this one before, don't add it.
		if _, ok := storyIDMap[event.FullDocument.StoryID]; !ok {
			// This is new, add it!
			storyIDMap[event.FullDocument.StoryID] = struct{}{}

			// Add it to the list of dirty story id's.
			dirty.StoryIDs = append(dirty.StoryIDs, event.FullDocument.StoryID)
		}

		// If we've already seen this one before, don't add it.
		if _, ok := userIDMap[event.FullDocument.AuthorID]; !ok {
			// This is new, add it!
			userIDMap[event.FullDocument.AuthorID] = struct{}{}

			// Add it to the list of dirty user id's.
			dirty.UserIDs = append(dirty.UserIDs, event.FullDocument.AuthorID)
		}
	}

	// Reset the underlying slice.
	w.events = make([]WatchEvent, 0)

	return &dirty
}
