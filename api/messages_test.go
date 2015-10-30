package api

import (
	"testing"
	log "github.com/Sirupsen/logrus"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestTextMessageToByteArray(t *testing.T) {
	msg := NewTextMessage("foobar")
	bytes := msg.ToByteArray()
	expectedMsg := "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"foobar"

	log.Debugln("original msg: ", bytes)
	log.Debugln("expected msg: ", []byte(expectedMsg))

	if msg != expectedMsg {

	}


}
