package main


type Converter struct {
}

type SpringMessageConverter interface {
	ConvertMessage(payload []byte) []byte
}


//   format: 0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]
//   sample: "\xff\x01\x0bcontentType\x00\x00\x00\x0c\"text/plain\"2015-10-25 23:13:21"

func (c Converter) ConvertMessage(payload []byte) []byte {
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
	payloadBytes := []byte(payload)
	serializedPayload := append(contentTypeHeader, payloadBytes...)

	// append serialized headers and payload
	return append(preambledHeader, serializedPayload...);
}
