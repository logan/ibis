package datastore

import "strconv"
import "time"

import "github.com/sdming/gosnow"

var Epoch = time.Date(2014, 1, 0, 0, 0, 0, 0, time.UTC)

func init() {
	gosnow.Since = Epoch.UnixNano() / 1000000
}

type SeqID string

type SeqIDGenerator interface {
	New() (SeqID, error)
	CurrentInterval() string
}

type snowflakeGenerator struct {
	snowflake *gosnow.SnowFlake
}

func NewSeqIDGenerator() (SeqIDGenerator, error) {
	snowflake, err := gosnow.Default()
	if err != nil {
		return nil, err
	}
	return &snowflakeGenerator{snowflake}, nil
}

func (s *snowflakeGenerator) New() (SeqID, error) {
	// TODO: catch and retry on error
	uid, err := s.snowflake.Next()
	if err != nil {
		return "", err
	}
	// TODO: pad to 13 characters
	return SeqID(strconv.FormatUint(uid, 36)), nil
}

func (s *snowflakeGenerator) CurrentInterval() string {
	shift := uint64(gosnow.WorkerIdBits + gosnow.SequenceBits)
	i := uint64(time.Now().UnixNano()/1000000-gosnow.Since) << shift
	return interval(SeqID(strconv.FormatUint(i, 36)))
}

func interval(seqID SeqID) string {
	// drop last 8 characters
	s := string(seqID)
	if len(s) <= 8 {
		return "0"
	}
	return s[:len(s)-8]
}

func intervalToSeqID(interval string) SeqID {
	return SeqID(interval + "00000000")
}

func decrInterval(interval string) string {
	i, _ := strconv.ParseUint(interval, 36, 64)
	return strconv.FormatUint(i-1, 36)
}

func incrInterval(interval string) string {
	i, _ := strconv.ParseUint(interval, 36, 64)
	return strconv.FormatUint(i+1, 36)
}
