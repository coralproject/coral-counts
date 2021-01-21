package coral

type CommentCounts struct {
	Action map[string]int `bson:"action"`
	Status struct {
		Approved       int `bson:"APPROVED"`
		None           int `bson:"NONE"`
		Premod         int `bson:"PREMOD"`
		Rejected       int `bson:"REJECTED"`
		SystemWithheld int `bson:"SYSTEM_WITHHELD"`
	} `bson:"status"`
	ModerationQueue struct {
		Total  int `bson:"total"`
		Queues struct {
			Unmoderated int `bson:"unmoderated"`
			Reported    int `bson:"reported"`
			Pending     int `bson:"pending"`
		} `bson:"queues"`
	} `bson:"moderationQueue"`
}
