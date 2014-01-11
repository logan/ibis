package datastore

import "strconv"
import "time"

import "github.com/sdming/gosnow"

type SeqID string

type SeqIDGenerator struct {
	snowflake *gosnow.SnowFlake
}

func NewSeqIDGenerator() (*SeqIDGenerator, error) {
	snowflake, err := gosnow.Default()
	if err != nil {
		return nil, err
	}
	return &SeqIDGenerator{snowflake}, nil
}

func (s *SeqIDGenerator) New() (SeqID, error) {
	// TODO: catch and retry on error
	uid, err := s.snowflake.Next()
	if err != nil {
		return "", err
	}
	// TODO: pad to 13 characters
	return SeqID(strconv.FormatUint(uid, 36)), nil
}

func interval(seqID SeqID) string {
	// drop last 8 characters
	s := string(seqID)
	return s[:len(s)-8]
}

func currentInterval() string {
	shift := uint64(gosnow.WorkerIdBits + gosnow.SequenceBits)
	i := uint64(time.Now().UnixNano()/1000000-gosnow.Since) << shift
	return interval(SeqID(strconv.FormatUint(i, 36)))
}

func decrInterval(interval string) string {
	i, _ := strconv.ParseUint(interval, 36, 64)
	return strconv.FormatUint(i-1, 36)
}

func incrInterval(interval string) string {
	i, _ := strconv.ParseUint(interval, 36, 64)
	return strconv.FormatUint(i+1, 36)
}
