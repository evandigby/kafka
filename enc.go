package main

import (
	"encoding/binary"
	"io"
)

func readNullableString(r io.Reader) (*string, error) {
	var strLen int16
	err := binary.Read(r, binary.BigEndian, &strLen)
	if err != nil {
		return nil, err
	}

	if strLen < 0 {
		return nil, nil
	}

	buf := make([]byte, int(strLen))
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}

	str := string(buf)

	return &str, nil
}

func readString(r io.Reader) (string, error) {
	var strLen int16
	err := binary.Read(r, binary.BigEndian, &strLen)
	if err != nil {
		return "", err
	}

	buf := make([]byte, int(strLen))
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func writeString(w io.Writer, s string) error {
	err := binary.Write(w, binary.BigEndian, int16(len(s)))
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(s))
	return err
}

func stringSize(s string) int32 {
	return int32(len(s)) + 2
}

func writeStringArray(w io.Writer, strings []string) error {
	if len(strings) == 0 {
		return binary.Write(w, binary.BigEndian, int32(-1))
	}

	err := binary.Write(w, binary.BigEndian, int32(len(strings)))
	if err != nil {
		return err
	}

	for _, s := range strings {
		err = writeString(w, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func stringArraySize(strings []string) int32 {
	sz := int32(4)
	for _, s := range strings {
		sz += stringSize(s)
	}
	return sz
}

func boolSize() int32 {
	return 1
}

func writeBool(w io.Writer, value bool) error {
	if !value {
		_, err := w.Write([]byte{0})
		return err
	}

	_, err := w.Write([]byte{1})
	return err
}
