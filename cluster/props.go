package cluster

// UserProperty is one MQTT 5 user property. A message can carry several with
// the same key, so Props keeps them as an ordered slice and not a map.
type UserProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Props holds the MQTT 5 properties of a publish that must survive a trip
// through the cell: a will message, a log entry or a routed message carries
// them. The zero value means no properties.
//
// A later task adds the encoding of Props to and from NATS message headers.
type Props struct {
	// User is the user properties in the order the publisher sent them.
	// Duplicate keys are allowed and kept.
	User []UserProperty `json:"user,omitempty"`
	// ContentType is the content type, "" when absent.
	ContentType string `json:"content_type,omitempty"`
	// ResponseTopic is the response topic, "" when absent.
	ResponseTopic string `json:"response_topic,omitempty"`
	// CorrelationData is the correlation data, empty when absent.
	CorrelationData []byte `json:"correlation_data,omitempty"`
	// MessageExpiry is the message expiry interval in seconds, 0 for none.
	MessageExpiry uint32 `json:"message_expiry,omitempty"`
	// PayloadFormat is the payload format indicator, nil when absent. Nil and
	// a pointer to 0 differ: the first says the publisher sent none.
	PayloadFormat *byte `json:"payload_format,omitempty"`
}

// IsZero reports whether p carries no properties at all. encoding/json uses it
// for the omitzero tag.
func (p Props) IsZero() bool {
	return len(p.User) == 0 && p.ContentType == "" && p.ResponseTopic == "" &&
		len(p.CorrelationData) == 0 && p.MessageExpiry == 0 && p.PayloadFormat == nil
}
