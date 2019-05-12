package enc

import (
	"encoding/binary"
	"io"
	"time"
)

// ReadNullableString reads a nullable string from the reader
func ReadNullableString(r io.Reader) (*string, error) {
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

// ReadString reads a string from the reader
func ReadString(r io.Reader) (string, error) {
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

// WriteString writes a string to the writer
func WriteString(w io.Writer, s string) error {
	err := binary.Write(w, binary.BigEndian, int16(len(s)))
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(s))
	return err
}

// StringSize returns the encoded size of the string
func StringSize(s string) int32 {
	return int32(len(s)) + 2
}

// WriteStringArray writes an array of strings to the writer
func WriteStringArray(w io.Writer, strings []string) error {
	err := binary.Write(w, binary.BigEndian, int32(len(strings)))
	if err != nil {
		return err
	}

	for _, s := range strings {
		err = WriteString(w, s)
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteNullableStringArray writes an array of nullable strings to the writer
func WriteNullableStringArray(w io.Writer, strings []string) error {
	if len(strings) == 0 {
		return binary.Write(w, binary.BigEndian, int32(-1))
	}

	err := binary.Write(w, binary.BigEndian, int32(len(strings)))
	if err != nil {
		return err
	}

	for _, s := range strings {
		err = WriteString(w, s)
		if err != nil {
			return err
		}
	}
	return nil
}

// StringArraySize returns the encoded size of the string array
func StringArraySize(strings []string) int32 {
	sz := int32(4)
	for _, s := range strings {
		sz += StringSize(s)
	}
	return sz
}

// BoolSize returns the encoded size of a bool
func BoolSize() int32 {
	return 1
}

// ReadBool reads a bool value from the reader
func ReadBool(r io.Reader) (bool, error) {
	var v bool
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// ReadDuration reads a duration from the reader (assuming value is stored in milliseconds)
func ReadDuration(r io.Reader) (time.Duration, error) {
	v, err := ReadInt32(r)
	return time.Duration(v) * time.Millisecond, err
}

// WriteBool writes a bool value to the writer
func WriteBool(w io.Writer, value bool) error {
	data := []byte{0}
	if value {
		data[0] = 1
	}

	_, err := w.Write(data)
	return err
}

// ReadInt32 reads an int32 from the reader
func ReadInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// ReadInt16 reads an int16 from the reader
func ReadInt16(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

// ElementReader reads an element from the reader for the specified version
type ElementReader interface {
	Read(r io.Reader, v int16) (err error)
}

// LengthFunc is called when we know the length of an array
type LengthFunc func(l int)

// ElementFunc is called when we have received an element of the array
type ElementFunc func(i int) ElementReader

// Array is used to decode a generic array
func Array(r io.Reader, v int16, lf LengthFunc, f ElementFunc) error {
	arrLen, err := ReadInt32(r)
	if err != nil {
		return err
	}

	lf(int(arrLen))

	for i := 0; i < int(arrLen); i++ {
		er := f(i)
		err := er.Read(r, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadInt32Array reads an int32 array
func ReadInt32Array(r io.Reader) ([]int32, error) {
	arrLen, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	resp := make([]int32, int(arrLen))

	for i := 0; i < int(arrLen); i++ {
		val, err := ReadInt32(r)
		if err != nil {
			return nil, err
		}
		resp[i] = val
	}

	return resp, nil
}
