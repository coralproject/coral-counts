package counts

type CommentStatusCounts struct {
	Approved       int `bson:"APPROVED"`
	None           int `bson:"NONE"`
	Premod         int `bson:"PREMOD"`
	Rejected       int `bson:"REJECTED"`
	SystemWithheld int `bson:"SYSTEM_WITHHELD"`
}

func (csc *CommentStatusCounts) Increment(comment *Comment) {
	switch comment.Status {
	case "APPROVED":
		csc.Approved++
	case "NONE":
		csc.None++
	case "PREMOD":
		csc.Premod++
	case "REJECTED":
		csc.Rejected++
	case "SYSTEM_WITHHELD":
		csc.SystemWithheld++
	}
}

type CommentModerationQueue struct {
	Total  int `bson:"total"`
	Queues struct {
		Unmoderated int `bson:"unmoderated"`
		Reported    int `bson:"reported"`
		Pending     int `bson:"pending"`
	} `bson:"queues"`
}

func (cmq *CommentModerationQueue) Increment(comment *Comment) {
	switch comment.Status {
	case "NONE":
		cmq.Total++
		cmq.Queues.Unmoderated++

		// If this comment has a flag on it, then it should also be in the reported
		// queue.
		if count, ok := comment.ActionCounts["FLAG"]; ok && count > 0 {
			cmq.Queues.Reported++
		}
	case "PREMOD":
		cmq.Total++
		cmq.Queues.Unmoderated++
		cmq.Queues.Pending++
	case "SYSTEM_WITHHELD":
		cmq.Total++
		cmq.Queues.Unmoderated++
		cmq.Queues.Pending++
	}
}

type CommentActionCounts map[string]int

func (cac CommentActionCounts) Increment(comment *Comment) {
	for key, count := range comment.ActionCounts {
		cac[key] += count
	}
}
