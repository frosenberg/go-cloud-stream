package api

import (
	log "github.com/Sirupsen/logrus"
	"reflect"
	"testing"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestTextMessageToByteArray(t *testing.T) {
	actualMsg := NewTextMessage([]byte("foobar"))
	actualMsgBytes := actualMsg.ToRawByteArray()
	expectedMsg := "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"foobar"
	expectedMsgBytes := []byte(expectedMsg)

	//	log.Debugln("  actual msg: ", actualMsgBytes)
	//	log.Debugln("expected msg: ", []byte(expectedMsg))

	if !reflect.DeepEqual(actualMsgBytes, expectedMsgBytes) {
		t.Fatalf("Messages are not matching")
	}
}

func TestGenericMessageToByteArray(t *testing.T) {
	actualHeaders := make(map[string]string)
	actualHeaders["contentType"] = "\"text/plain\""
	actualHeaders["originalContentType"] = "\"application/json\""

	actualMsg := NewMessage(actualHeaders, []byte("{ foor : bar }"))
	actualMsgBytes := actualMsg.ToRawByteArray()
	expectedMsg := "\xff\x02\x0bcontentType\x00\x00\x00\x0c\"text/plain\"\x13originalContentType\x00\x00\x00\x12\"application/json\"{ foor : bar }"
	expectedMsgBytes := []byte(expectedMsg)

	//log.Debugln("  actual msg: ", actualMsgBytes)
	//log.Debugln("expected msg: ", []byte(expectedMsg))

	if !reflect.DeepEqual(actualMsgBytes, expectedMsgBytes) {
		t.Fatalf("Messages are not matching")
	}
}

func TestByteArrayToMessage(t *testing.T) {
	// message coming out of Sprint http source module (from Redis) with this call
	// curl -XPOST -H"Content-Type: application/json" localhost:9999/messages -d "{ "foor" : "bar" }"
	actualMsgBytesString := "\xff\x02\x0bcontentType\x00\x00\x00\x0c\"text/plain\"\x13originalContentType\x00\x00\x00\x12\"application/json\"{ foor : bar }"
	actualMsgBytes := []byte(actualMsgBytesString)
	actualMsg := NewMessageFromRawBytes(actualMsgBytes)

	//log.Debugln("actualMsg header: ", actualMsg)

	expectedHeaders := make(map[string]string)
	expectedHeaders["contentType"] = "\"text/plain\""
	expectedHeaders["originalContentType"] = "\"application/json\""

	if !reflect.DeepEqual(actualMsg.Headers, expectedHeaders) {
		t.Fatalf("Message headers not equals")
	}
	if !reflect.DeepEqual(actualMsg.Content, []byte("{ foor : bar }")) {
		t.Fatalf("Message content not equals")
	}
}

func TestMessageRoundtrip(t *testing.T) {
	actualMsgBytesString := "\xff\x02\x0bcontentType\x00\x00\x00\x0c\"text/plain\"\x13originalContentType\x00\x00\x00\x12\"application/json\"{ foor : bar }"
	actualMsgBytes := []byte(actualMsgBytesString)
	actualMsg := NewMessageFromRawBytes(actualMsgBytes)

	if !reflect.DeepEqual(actualMsg.ToRawByteArray(), actualMsgBytes) {
		t.Fatalf("Message roundtripping failed")
	}
}
