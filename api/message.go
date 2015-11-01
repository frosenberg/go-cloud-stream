package api

import (
	"encoding/binary"
//	log "github.com/Sirupsen/logrus"
	"sort"
	"net/http"
	"io/ioutil"
	"fmt"
)

// Message type used to send a receive messages between Transports
type Message struct {
	Headers map[string]string
	Content []byte
}

func (c Message) String() string {
	return fmt.Sprintf("[%s, %s]", c.Headers, c.Content)
}

//
// Creates a new message with a custom set of headers and
// a byte array with the content.
//
func NewMessage(headers map[string]string, content []byte) *Message {
	return &Message { Headers : headers,
		Content: content}
}

//
// Creates a new Message instance with a given string content.
//
func NewTextMessage(content string) *Message {
	headers := make(map[string]string)
	headers["contentType"] = "\"text/plain\""
	return &Message { Headers : headers,
					  Content: []byte(content)}
}

//
// Creates a new Message instance with a given http request.
//
func NewMessageFromHttpRequest(r *http.Request) *Message {
	ct := r.Header.Get("Content-Type")
	if ct == "" {
		ct = r.Header.Get("content-type")
	}

	headers := make(map[string]string)
	headers["contentType"] = "\"text/plain\""
	headers["originalContentType"] = fmt.Sprintf("\"" + ct + "\"")

	body, _ := ioutil.ReadAll(r.Body)

	return &Message{ Headers: headers, Content: body}
}

//
// Converts a raw byte array received from the transport to a Message instance.
// This message is only required when building a new Transport.
//
func NewMessageFromRawBytes(rawData []byte) *Message {
	return toMessage(rawData);
}

// This methods transforms a raw byte array that is read from the Transport
// to a Message object. This is only required when building a transport.
//
// Format: 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
// Sample: "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"2015-10-25 23:13:21"
func toMessage(payload []byte) *Message {
	message := &Message { Headers : make(map[string]string) }

	if len(payload) == 0 {
		return message
	}

	headerCount := int(payload[1])
	headerPos := 2

//	log.Debugln("Message::toMessage - headerCount: ", headerCount)

	// TODO test with more than 1 header ;)
	for i := 0; i < headerCount; i++ {
		// determine length of header name (e.g., contentType)
		headerLength := int(payload[headerPos])
		headerNameStartIndex := headerPos + 1
		headerNameEndIndex := headerNameStartIndex + headerLength
		headerName := payload[headerNameStartIndex:headerNameEndIndex]

		// update position
		headerPos = headerNameEndIndex

		// its 4 bytes of header value length field
		headerValueLength := int(binary.BigEndian.Uint32(payload[headerPos:headerPos+4]))


		// update position
		headerPos = headerPos+4;

		headerValue := payload[headerPos:headerPos+headerValueLength]
		message.Headers[string(headerName)] = string(headerValue)

		// update position
		headerPos = headerPos + headerValueLength
	}

	message.Content = payload[headerPos:len(payload)]
	return message
}

//   This method converts a Message to a raw byte array that is expected
//   by the underlying transport. This message is only required when building
//   a new transport.
//
//   Format: 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
//   Sample: "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"2015-10-25 23:13:21"
func (m *Message) ToRawByteArray() []byte {
	preamble := make([]byte, 2)

	// build preamble
	preamble[0] = 0xff // signature
	preamble[1] = byte(len(m.Headers)) // number of headers

	payload := make([]byte, 0)

	// sort map to ensure we generate headers in order
	var keys []string
	for k, _ := range m.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// [lenHdr(1), hdr, lenValue(4), value]
	for _, k := range keys {
		header := make([]byte, 0)

		// append length of the header name
		header = append(header, byte(len(k)))

		// append byte array of the header name itself
		header = append(header, []byte(k)...)

		// determine header value len (pad to 4 byte) and append
		headerValueLen := make([]byte, 4)

		v := m.Headers[k]

		binary.BigEndian.PutUint32(headerValueLen, uint32(len(v)))

		header = append(header, headerValueLen...)
		header = append(header, []byte(v)...)
		payload = append(payload, header...)
	}

	payload = append(payload, m.Content...)
	return append(preamble, payload...)
}
