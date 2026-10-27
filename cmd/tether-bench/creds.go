package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
)

// cred is one device's MQTT username (the key ID) and password (the raw key).
type cred struct{ User, Pass string }

// readCreds reads a CSV of device credentials: username and password per row,
// with an optional header row that names them. Extra columns are ignored.
func readCreds(path string) ([]cred, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	var out []cred

	for i, row := range rows {
		if len(row) < 2 {
			return nil, fmt.Errorf("%s: line %d: want username,password", path, i+1)
		}

		if i == 0 && row[0] == "username" {
			continue
		}

		out = append(out, cred{User: row[0], Pass: row[1]})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no credentials", path)
	}

	return out, nil
}

// writeCreds writes creds as a CSV with a header row. The file holds raw device
// keys, so it is readable by its owner only.
func writeCreds(path string, creds []cred) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)

	werr := w.Write([]string{"username", "password"})

	for _, c := range creds {
		if werr != nil {
			break
		}

		werr = w.Write([]string{c.User, c.Pass})
	}

	w.Flush()

	return errors.Join(werr, w.Error(), f.Close())
}
