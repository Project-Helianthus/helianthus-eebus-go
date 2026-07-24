package service

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCanonicalExternalConsumerCompilesLegacyAndAdditiveSurfaces(t *testing.T) {
	moduleRoot, err := filepath.Abs("..")
	require.NoError(t, err)
	fixtureRoot := t.TempDir()
	goMod := fmt.Sprintf(`module example.com/eebus-compatibility

go 1.22.0

require github.com/Project-Helianthus/helianthus-eebus-go v0.0.0

replace github.com/Project-Helianthus/helianthus-eebus-go => %s
`, strconv.Quote(filepath.ToSlash(moduleRoot)))
	require.NoError(t, os.WriteFile(filepath.Join(fixtureRoot, "go.mod"), []byte(goMod), 0o600))
	require.NoError(t, os.WriteFile(
		filepath.Join(fixtureRoot, "compatibility_test.go"),
		[]byte(externalCompatibilityFixture),
		0o600,
	))

	output := runExternalGoCommand(t, fixtureRoot, "test", "./...")
	assertNoUpstreamForkIdentity(t, output)
	assertNoUpstreamForkIdentity(t, runExternalGoCommand(t, fixtureRoot, "list", "-m", "all"))
	assertNoUpstreamForkIdentity(t, runExternalGoCommand(t, fixtureRoot, "list", "-deps", "./..."))
}

func runExternalGoCommand(t *testing.T, directory string, arguments ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "go", arguments...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOFLAGS=-mod=mod")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s\n%s", command.String(), output)
	return string(output)
}

func assertNoUpstreamForkIdentity(t *testing.T, value string) {
	t.Helper()
	for _, module := range []string{"eebus-go", "ship-go", "spine-go"} {
		forbidden := "github.com/" + "enbility/" + module
		require.NotContains(t, value, forbidden)
	}
}

const externalCompatibilityFixture = `package compatibility

import (
	"context"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	"github.com/Project-Helianthus/helianthus-eebus-go/service"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	shipmodel "github.com/Project-Helianthus/helianthus-ship-go/model"
)

type legacyReader struct{}

func (*legacyReader) RemoteSKIConnected(api.ServiceInterface, string) {}
func (*legacyReader) RemoteSKIDisconnected(api.ServiceInterface, string) {}
func (*legacyReader) VisibleRemoteServicesUpdated(api.ServiceInterface, []shipapi.RemoteService) {}
func (*legacyReader) ServiceShipIDUpdate(string, string) {}
func (*legacyReader) ServicePairingDetailUpdate(string, *shipapi.ConnectionStateDetail) {}

type gate struct{}

func (*gate) Prepare(shipapi.OutgoingAttemptRequest) (shipapi.OutgoingAttemptHandle, error) {
	return nil, nil
}

func (*gate) AuthorizeLaunch(shipapi.OutgoingAttemptHandle) (shipapi.OutgoingAttemptPermit, error) {
	return shipapi.OutgoingAttemptPermit{Context: context.Background()}, nil
}

func (*gate) AbortPrepared(shipapi.OutgoingAttemptHandle) (shipapi.OutgoingAttemptAbortResult, error) {
	return shipapi.OutgoingAttemptAbortStaleNoOp, nil
}

type sink struct{}

func (*sink) OutgoingAttemptConnectionClosed(string, bool, shipapi.OutgoingAttemptMetadata) {}
func (*sink) OutgoingAttemptHandshakeStateUpdate(string, shipmodel.ShipState, shipapi.OutgoingAttemptMetadata) {}

var legacyConstructor func(*api.Configuration, api.ServiceReaderInterface) *service.Service = service.NewService
var additiveConstructor func(
	*api.Configuration,
	api.ServiceReaderInterface,
	service.OutgoingAttemptBridgeConfiguration,
) *service.Service = service.NewServiceWithOutgoingAttemptBridge

var _ api.ServiceInterface = (*service.Service)(nil)
var _ api.PairingCandidateQueuer = (*service.Service)(nil)
var _ api.ServiceReaderInterface = (*legacyReader)(nil)
var _ shipapi.OutgoingAttemptGate = (*gate)(nil)
var _ shipapi.OutgoingAttemptHubReaderInterface = (*sink)(nil)

func compile(configuration *api.Configuration, reader api.ServiceReaderInterface) {
	var legacy api.ServiceInterface = legacyConstructor(configuration, reader)
	_ = legacy
	bridge := service.OutgoingAttemptBridgeConfiguration{Gate: &gate{}, Sink: &sink{}}
	var additive api.ServiceInterface = additiveConstructor(configuration, reader, bridge)
	_ = additive
}
`
