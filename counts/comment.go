package counts

// Comment is a Comment in Coral.
type Comment struct {
	AuthorID     string         `bson:"authorID"`
	StoryID      string         `bson:"storyID"`
	Status       string         `bson:"status"`
	ActionCounts map[string]int `bson:"actionCounts"`
}
