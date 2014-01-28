package ibis

import "fmt"
import "strconv"
import "time"

import "github.com/sdming/gosnow"

// Snowflakes are derived from millisecond timestamps relative to the beginning of 2014.
var Epoch = time.Date(2014, 1, 0, 0, 0, 0, 0, time.UTC)

func init() {
	gosnow.Since = Epoch.UnixNano() / 1000000
}

// A SeqID is a base-36 string that is intended to uniquely identify a sort objects.
type SeqID string

func (s SeqID) Pad() SeqID {
	return SeqID(fmt.Sprintf("%013s", s))
}

// SeqIDGenerator provides a way to generate SeqIDs. They should be unique and roughly ascending.
type SeqIDGenerator interface {
	NewSeqID() (SeqID, error)
}

type snowflakeGenerator struct {
	snowflake *gosnow.SnowFlake
}

// NewSeqIDGenerator returns a SeqIDGenerator using github.com/sdming/snowflake as its
// implementation.
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
