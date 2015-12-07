package transport

import (
	"github.com/frosenberg/go-cloud-stream/api"
	"testing"
)

func TestGetBindingSemantic(t *testing.T) {

	if GetBindingSemantic("foo") != api.QueueSemantic ||
		GetBindingSemantic("queue:fooqueue") != api.QueueSemantic {
		t.Fatalf("Incorrect queue binding semantic")
	}

	if GetBindingSemantic("topic:topicfoo") != api.TopicSemantic ||
		GetBindingSemantic("topic:foo") != api.TopicSemantic {
		t.Fatalf("Incorrect topic binding semantic")
	}
}

func TestIsTopicPrefix(t *testing.T) {

	if IsTopicPrefix("queue:foo") &&
		!IsTopicPrefix("topic:foo") &&
		!IsTopicPrefix("topic:topicfoo") {
		t.Fatalf("Incorrect topic prefix")
	}

}

func TestIsQueuePrefix(t *testing.T) {

	if !IsQueuePrefix("queue:foo") &&
		IsQueuePrefix("topic:foo2") {
		t.Fatalf("Incorrect queue prefix")
	}
}

func TestPrefix(t *testing.T) {

	if actual := Prefix("foobar"); actual != "queue.foobar" {
		t.Fatalf("Prefix 'foobar' incorrect. Got: %s", actual)
	}
	if actual := Prefix("queue_foobar"); actual != "queue.queue_foobar" {
		t.Fatalf("Prefix 'queue_foobar' incorrect. Got: %s", actual)
	}
	if actual := Prefix("topic:foobar"); actual != "topic.foobar" {
		t.Fatalf("Prefix 'topic:foobar' incorrect. Got: %s", actual)
	}
	if actual := Prefix("topic_foobar"); actual != "queue.topic_foobar" {
		t.Fatalf("Prefix 'topic_foobar' incorrect. Got: %s", actual)
	}
}

func TestStripPrefix(t *testing.T) {

	if actual := StripPrefix("fooprefix:topicname"); actual != "fooprefix:topicname" {
		t.Fatalf("Incorrect striping. Got: %s", actual)
	}

	if actual := StripPrefix("topic:topicname"); actual != "topicname" {
		t.Fatalf("Incorrect striping. Got: %s", actual)
	}

	if actual := StripPrefix("queue:queuename"); actual != "queuename" {
		t.Fatalf("Incorrect striping. Got: %s", actual)
	}
}

func TestEscapeTopic(t *testing.T) {

	if actual := EscapeTopicName("queue:foobar"); actual != "queue_3Afoobar" {
		t.Fatalf("Incorrect escapting. Got: %s", actual)
	}
	if actual := EscapeTopicName("foobar"); actual != "foobar" {
		t.Fatalf("Incorrect escapting. Got: %s", actual)
	}

}
