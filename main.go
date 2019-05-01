package main

import (
	"bytes"
	"crypto/tls"
	"fmt"
)

func main() {
	conn, err := tls.Dial("tcp", "", nil)
	if err != nil {
		fmt.Println("Dial Error", err)
		return
	}

	defer conn.Close()

	const clientID = "testClient"
	const (
		versionsCID  = 10
		handshakeCID = 11
		authCID      = 12
	)

	request := newAPIVersionsRequest(versionsCID, clientID)

	err = request.write(conn)
	if err != nil {
		fmt.Println("err writing version request:", err)
		return
	}

	cid, resp, err := readResponse(conn, true)
	if err != nil {
		fmt.Println("err reading response:", err)
		return
	}

	if cid != versionsCID {
		fmt.Println("uncorrelated response:", cid)
		return
	}

	err = readAPIVersionsResponse(bytes.NewBuffer(resp))
	if err != nil {
		fmt.Println("err reading versions response:", err)
		return
	}

	request = newSASLHandshakeRequest(handshakeCID, clientID, "PLAIN")
	err = request.write(conn)
	if err != nil {
		fmt.Println("err sending metadata request:", err)
		return
	}

	cid, resp, err = readResponse(conn, true)
	if err != nil {
		fmt.Println("err reading handshake response:", err)
		return
	}

	if cid != handshakeCID {
		fmt.Println("uncorrelated response:", cid)
		return
	}

	err = readSASLHandshakeResponse(bytes.NewBuffer(resp))
	if err != nil {
		fmt.Println("err reading handshake response:", err)
		return
	}

	request = newSASLAuthenticateRequest("$ConnectionString", "")
	err = request.write(conn)
	if err != nil {
		fmt.Println("err sending metadata request:", err)
		return
	}

	cid, resp, err = readResponse(conn, false)
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}

	err = readSASLHandshakeResponse(bytes.NewBuffer(resp))
	if err != nil {
		fmt.Println("err reading auth response:", err)
		return
	}

	fmt.Println("AUTH", resp)
}
