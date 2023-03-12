package log

type Config struct {
	Segment struct {
		MaxScoreBytes uint64
		MaxIndexBytes uint64
		InitialOffset uint64
	}
}
