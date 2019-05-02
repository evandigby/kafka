package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"os"
	"time"
)

func main() {
	host := os.Getenv("KAFKA_BOOTSTRAP")
	conn, err := tls.Dial("tcp", host, nil)
	if err != nil {
		fmt.Println("Dial Error", err)
		return
	}

	conn.SetDeadline(time.Time{})
	defer conn.Close()

	const clientID = "testClient"
	const (
		versionsCID  = 10
		handshakeCID = 11
		metaCID      = 12
		apiCID       = 13
	)

	request := newSASLHandshakeRequest(handshakeCID, clientID, "PLAIN")
	err = request.write(conn)
	if err != nil {
		fmt.Println("err sending metadata request:", err)
		return
	}

	cid, resp, err := readResponse(conn, true)
	if err != nil {
		fmt.Println("err reading handshake response:", err)
		return
	}

	if cid != handshakeCID {
		fmt.Println("uncorrelated response:", cid)
		return
	}

	err = readSASLHandshakeResponse(bytes.NewReader(resp))
	if err != nil {
		fmt.Println("err reading handshake response:", err)
		return
	}

	userName := os.Getenv("KAFKA_USERNAME")
	password := os.Getenv("KAFKA_PASSWORD")

	request = newSASLAuthenticateRequest(userName, password)
	err = request.write(conn)
	if err != nil {
		fmt.Println("err sending auth request:", err)
		return
	}

	_, resp, err = readResponse(conn, false)
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}

	request = newMetadataRequest(metaCID, clientID, []string{"addfiretoposts"}, false)
	err = request.write(conn)
	if err != nil {
		fmt.Println("err sending metadata request:", err)
		return
	}

	cid, resp, err = readResponse(conn, true)
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}

	if cid != metaCID {
		fmt.Println("uncorrelated response:", cid)
		return
	}

	err = readMetadataResponse(bytes.NewReader(resp))
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}

	request = newAPIVersionsRequest(apiCID, clientID)
	err = request.write(conn)
	if err != nil {
		fmt.Println("err sending metadata request:", err)
		return
	}

	cid, resp, err = readResponse(conn, true)
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}

	if cid != apiCID {
		fmt.Println("uncorrelated response:", cid)
		return
	}

	err = readAPIVersionsResponse(bytes.NewReader(resp))
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}
}
