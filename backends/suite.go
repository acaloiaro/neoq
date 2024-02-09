package backends

import (
	"context"
	"testing"

	"github.com/acaloiaro/neoq"
	"github.com/stretchr/testify/suite"
)

type NeoQTestSuite struct {
	suite.Suite
	NeoQ neoq.Neoq
}

// NewNeoQTestSuite constructs a new NeoQ test suite that can be used to test
// any impementation of the queue
func NewNeoQTestSuite(q neoq.Neoq) *NeoQTestSuite {
	n := new(NeoQTestSuite)
	n.NeoQ = q
	return n
}

func (s *NeoQTestSuite) Run(t *testing.T) {
	suite.Run(t, s)
}

func (s *NeoQTestSuite) TearDownSuite() {
	s.NeoQ.Shutdown(context.Background())
}
