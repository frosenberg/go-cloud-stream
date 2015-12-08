package transport

import (
	"bytes"
	"fmt"
	"github.com/frosenberg/go-cloud-stream/api"
	"strings"
)

// GetBindingSemantic returns the api.BindingSemantic for the
// binding string.
func GetBindingSemantic(binding string) api.BindingSemantic {
	if IsTopicPrefix(binding) {
		return api.TopicSemantic
	}
	return api.QueueSemantic
}

// IsTopicPrefix checks if the given string is the prefix
// for the topic semantic.
func IsTopicPrefix(binding string) bool {
	return strings.HasPrefix(binding, "topic:")
}

// IsQueuePrefix checks if the given string is the prefix
// for the queue semantic.
func IsQueuePrefix(binding string) bool {
	return strings.HasPrefix(binding, "queue:")
}

// Prefix set the prefix of a binding correctly as it is
// expected by the underlying Redis transport.
func Prefix(binding string) string {
	if IsTopicPrefix(binding) {
		return strings.Replace(binding, "topic:", "topic.", 1)
	}
	if IsQueuePrefix(binding) {
		return strings.Replace(binding, "queue:", "queue.", 1)
	}
	return fmt.Sprintf("queue.%s", binding)
}

// StripPrefix strips off the prefix entirely in case of "topic" or "queue".
// If the prefix cannot be found the original string is returned.
func StripPrefix(binding string) string {
	if IsTopicPrefix(binding) {
		return binding[6:]
	}
	if IsQueuePrefix(binding) {
		return binding[6:]
	}
	return binding
}

// EscapeTopicName escapes the name of the topic. allowed chars are ASCII
// alphanumerics, '.', '_' and '-'. '_' is used as escaped char in the form '_xx' where xx
// is the hexadecimal value of the byte(s) needed to represent an illegal char in utf8.
//
// Source : https://github.com/spring-cloud/spring-cloud-stream/blob/master/spring-cloud-stream-binders/spring-cloud-stream-binder-kafka/src/main/java/org/springframework/cloud/stream/binder/kafka/KafkaMessageChannelBinder.java#L401
func EscapeTopicName(topic string) string {
	var buffer bytes.Buffer

	byteArr := []byte(topic) // utf-8 byte seq
	for _, b := range byteArr {

		if (b >= 'a') && (b <= 'z') || (b >= 'A') && (b <= 'Z') || (b >= '0') && (b <= '9') || (b == '.') || (b == '-') {
			buffer.WriteByte(b)
		} else {
			buffer.WriteString(fmt.Sprintf("_%02X", b))
		}
	}
	return buffer.String()
}
