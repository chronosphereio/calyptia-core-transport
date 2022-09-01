package transport

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/go-kit/log"
)

func TestFailConnect(t *testing.T) {
	var logger log.Logger
	logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	uri, err := url.Parse("no://way.to.connect")
	if err != nil {
		t.Fatal(err)
	}

	_, err = Connect(context.Background(), logger, uri)
	if err == nil {
		t.Errorf("no error when connecting to non-existent site")
	}
}
