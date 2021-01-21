package internal

type CommentCounts struct {
	Action map[string]int64 `bson:"action"`
	Status struct {
		Approved       int64 `bson:"APPROVED"`
		None           int64 `bson:"NONE"`
		Premod         int64 `bson:"PREMOD"`
		Rejected       int64 `bson:"REJECTED"`
		SystemWithheld int64 `bson:"SYSTEM_WITHHELD"`
	} `bson:"status"`
	ModerationQueue struct {
		Total  int64 `bson:"total"`
		Queues struct {
			Unmoderated int64 `bson:"unmoderated"`
			Reported    int64 `bson:"reported"`
			Pending     int64 `bson:"pending"`
		} `bson:"queues"`
	} `bson:"moderationQueue"`
}
