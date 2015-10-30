package api

import (
	"encoding/binary"
	log "github.com/Sirupsen/logrus"
)

// Message type used to send a receive messages
// between Transports
type Message struct {
	Headers map[string]string
	Content string
}

// Message Constructors
func NewMessage(headers map[string]string, content string) *Message {
	return &Message { Headers : headers,
		Content: content}
}

func NewTextMessage(content string) *Message {
	headers := make(map[string]string)
	headers["contentType"] = "text/plain"
	return &Message { Headers : headers,
					  Content: content}
}

func NewMessageFromRawBytes(rawData []byte) *Message {
	return toMessage(rawData);
}

type MessageInterface interface {
	ToByteArray() []byte
}

//   format: 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
//   sample: "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"2015-10-25 23:13:21"
func toMessage(payload []byte) *Message {
	message := &Message { Headers : make(map[string]string) }

	if len(payload) == 0 {
		return message
	}

	headerCount := int(payload[1])
	headerPos := 2

	log.Debugln("Message::toMessage - headerCount: ", headerCount)

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

	message.Content = string(payload[headerPos:len(payload)])
	return message
}


//   format: 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
//   sample: "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"2015-10-25 23:13:21"
func (m *Message) ToByteArray() []byte {

	// TODO need to generate header correctly according to number in the Message

	preamble := make([]byte, 3)
	contentTypeHeaderName := []byte("contentType")
	contentTypeHeaderValue := []byte("\"text/plain\"")
	contentTypeHeaderLength := make([]byte, 4)

	// build preamble
	preamble[0] = 0xff // signature
	preamble[1] = 0x01 // number of headers

	// header name length (1-byte)
	preamble[2] = 0x0b // length of "contentType" header (0b == 11)

	// append header name
	preambledHeader := append(preamble, contentTypeHeaderName...)

	// header value length (4-bytes)
	contentTypeHeaderLength[0] = 0x00 // padding
	contentTypeHeaderLength[1] = 0x00 // padding
	contentTypeHeaderLength[2] = 0x00 // padding
	contentTypeHeaderLength[3] = 0x0c // length of header value "text/plain" (0c == 12)

	// append header value
	contentTypeHeader := append(contentTypeHeaderLength, contentTypeHeaderValue...)

	// append message payload
	payloadBytes := []byte(m.Content)
	serializedPayload := append(contentTypeHeader, payloadBytes...)

	// append serialized headers and payload
	return append(preambledHeader, serializedPayload...);
}
