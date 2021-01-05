// +build integration

package mmd

import (
	"os"
	"strconv"
	"testing"
)

var integrationTests = false

func init() {
	integrationTests, _ = strconv.ParseBool(os.Getenv("INTEGRATION"))
}

func TestEchoCall(t *testing.T) {
	if !integrationTests {
		t.Skip("integration tests disabled")
	}

	mmdc, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer closeConnection(t, mmdc)

	t.Log("Created mmd connection:", mmdc)
	resp, err := mmdc.Call("echo", "Howdy Doody")
	t.Logf("Response: %+v\nError: %v\n", resp, err)
}

func closeConnection(t *testing.T, mmdc Conn) {
	t.Log("Shutting down MMD connection")
	err := mmdc.close()
	t.Logf("Close error: %v\n", err)
}
