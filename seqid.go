package ibis

import "strconv"
import "time"

import "github.com/sdming/gosnow"

var Epoch = time.Date(2014, 1, 0, 0, 0, 0, 0, time.UTC)

func init() {
	gosnow.Since = Epoch.UnixNano() / 1000000
}

type SeqID string

type SeqIDGenerator interface {
	NewSeqID() (SeqID, error)
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

func (s *snowflakeGenerator) NewSeqID() (SeqID, error) {
	// TODO: catch and retry on error
	uid, err := s.snowflake.Next()
	if err != nil {
		return "", err
	}
	// TODO: pad to 13 characters
	return SeqID(strconv.FormatUint(uid, 36)), nil
}
