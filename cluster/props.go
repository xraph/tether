package cluster

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

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
// A log entry carries Props in NATS headers (see encode and decode below).
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

// The header names that carry Props on a log entry. Every value is
// base64url (unpadded) except the two numbers, so no value can contain a
// character a NATS header cannot hold, whatever the publisher put in it.
const (
	hdrPropUser          = "Tether-Prop-User"
	hdrPropContentType   = "Tether-Prop-Content-Type"
	hdrPropResponseTopic = "Tether-Prop-Response-Topic"
	hdrPropCorrelation   = "Tether-Prop-Correlation-Data"
	hdrPropExpiry        = "Tether-Prop-Message-Expiry"
	hdrPropFormat        = "Tether-Prop-Payload-Format"
)

var headerB64 = base64.RawURLEncoding

// encode writes p into h. It writes a header only for a property that is
// present, so the zero Props adds none.
//
// User properties go into one header, as base64url of their frames: for each
// property, the key and then the value, each as a uvarint length and the raw
// bytes. That keeps their order and any duplicate keys, and loses nothing for
// a value that holds CR, LF or bytes that are not UTF-8.
func (p Props) encode(h nats.Header) {
	if len(p.User) > 0 {
		var buf []byte

		for _, u := range p.User {
			buf = binary.AppendUvarint(buf, uint64(len(u.Key)))
			buf = append(buf, u.Key...)
			buf = binary.AppendUvarint(buf, uint64(len(u.Value)))
			buf = append(buf, u.Value...)
		}

		h.Set(hdrPropUser, headerB64.EncodeToString(buf))
	}

	if p.ContentType != "" {
		h.Set(hdrPropContentType, headerB64.EncodeToString([]byte(p.ContentType)))
	}

	if p.ResponseTopic != "" {
		h.Set(hdrPropResponseTopic, headerB64.EncodeToString([]byte(p.ResponseTopic)))
	}

	if len(p.CorrelationData) > 0 {
		h.Set(hdrPropCorrelation, headerB64.EncodeToString(p.CorrelationData))
	}

	if p.MessageExpiry != 0 {
		h.Set(hdrPropExpiry, strconv.FormatUint(uint64(p.MessageExpiry), 10))
	}

	if p.PayloadFormat != nil {
		h.Set(hdrPropFormat, strconv.FormatUint(uint64(*p.PayloadFormat), 10))
	}
}

// decode reads the properties encode wrote from h into p, replacing it. A
// header that is absent leaves the property at its zero value. A header that
// does not parse is an error.
func (p *Props) decode(h nats.Header) error {
	*p = Props{}

	if v := h.Get(hdrPropUser); v != "" {
		raw, err := headerB64.DecodeString(v)
		if err != nil {
			return fmt.Errorf("props: %s: %w", hdrPropUser, err)
		}

		if p.User, err = decodeUserProps(raw); err != nil {
			return fmt.Errorf("props: %s: %w", hdrPropUser, err)
		}
	}

	var err error

	if p.ContentType, err = decodeStringHeader(h, hdrPropContentType); err != nil {
		return err
	}

	if p.ResponseTopic, err = decodeStringHeader(h, hdrPropResponseTopic); err != nil {
		return err
	}

	if v := h.Get(hdrPropCorrelation); v != "" {
		if p.CorrelationData, err = headerB64.DecodeString(v); err != nil {
			return fmt.Errorf("props: %s: %w", hdrPropCorrelation, err)
		}
	}

	if v := h.Get(hdrPropExpiry); v != "" {
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return fmt.Errorf("props: %s: %w", hdrPropExpiry, err)
		}

		p.MessageExpiry = uint32(n)
	}

	if v := h.Get(hdrPropFormat); v != "" {
		n, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return fmt.Errorf("props: %s: %w", hdrPropFormat, err)
		}

		f := byte(n)
		p.PayloadFormat = &f
	}

	return nil
}

func decodeStringHeader(h nats.Header, name string) (string, error) {
	v := h.Get(name)
	if v == "" {
		return "", nil
	}

	raw, err := headerB64.DecodeString(v)
	if err != nil {
		return "", fmt.Errorf("props: %s: %w", name, err)
	}

	return string(raw), nil
}

func decodeUserProps(raw []byte) ([]UserProperty, error) {
	var out []UserProperty

	next := func() (string, error) {
		n, w := binary.Uvarint(raw)
		if w <= 0 {
			return "", errors.New("truncated frame")
		}

		rest := raw[w:]

		if n > uint64(len(rest)) {
			return "", errors.New("truncated frame")
		}

		size := int(n) // #nosec G115 -- n is at most len(rest), checked above.
		raw = rest[size:]

		return string(rest[:size]), nil
	}

	for len(raw) > 0 {
		k, err := next()
		if err != nil {
			return nil, err
		}

		v, err := next()
		if err != nil {
			return nil, err
		}

		out = append(out, UserProperty{Key: k, Value: v})
	}

	return out, nil
}
