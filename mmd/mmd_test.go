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

	mmdc, err := LocalConnect()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Created mmd connection:", mmdc)
	resp, err := mmdc.Call("echo", "Howdy Doody")
	t.Logf("Response: %+v\nError: %v\n", resp, err)
	t.Log("Shutting down MMD connection")
	mmdc.close()
}
