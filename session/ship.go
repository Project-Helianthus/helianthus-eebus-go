package session

import (
	"crypto/x509"
	"errors"
	"fmt"
	"strconv"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	"github.com/Project-Helianthus/helianthus-ship-go/hub"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

// Service.Setup builds its own mDNS manager. Compose the same SHIP and SPINE
// nodes here so the hub can use the instance's shutdown-safe browse results.
func (s *Instance) setupSHIP(cfg *api.Configuration, register bool) error {
	certificate := cfg.Certificate()
	if len(certificate.Certificate) == 0 {
		return errors.New("missing certificate")
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		return err
	}
	ski, err := cert.SkiFromCertificate(leaf)
	if err != nil {
		return err
	}
	local := shipapi.NewServiceDetails(ski)
	local.SetShipID(cfg.Identifier())
	local.SetDeviceType(string(cfg.DeviceType()))

	// Match the service's SPINE address construction (SPINE 1.3.0 §7.1.1.2).
	vendor := cfg.VendorCode()
	vendorType := "i"
	if _, err := strconv.Atoi(vendor); err != nil {
		vendorType = "n"
	}
	deviceAddress := fmt.Sprintf("d:_%s:%s_%s-%s", vendorType, vendor, cfg.DeviceModel(), cfg.DeviceSerialNumber())
	if len(deviceAddress) > 256 {
		return fmt.Errorf("generated device address may not be longer than 256 characters: %s", deviceAddress)
	}
	device := spine.NewDeviceLocal(cfg.DeviceBrand(), cfg.DeviceModel(), cfg.DeviceSerialNumber(),
		cfg.Identifier(), deviceAddress, cfg.DeviceType(), cfg.FeatureSet())
	for _, entityType := range cfg.EntityTypes() {
		address := []model.AddressEntityType{model.AddressEntityType(len(device.Entities()))}
		device.AddEntity(spine.NewEntityLocal(device, entityType, address, cfg.HeartbeatTimeout()))
	}
	s.ski = local.SKI()
	s.localDevice = model.AddressDeviceType(deviceAddress)
	s.device = device
	s.hub = hub.NewHub(shipReader{Instance: s, register: register}, &s.mdns, cfg.Port(), certificate, local)
	return nil
}

type shipReader struct {
	*Instance
	register bool
}

var _ shipapi.HubReaderInterface = shipReader{}

func (r shipReader) RemoteSKIConnected(ski string) {
	r.Instance.RemoteSKIConnected(nil, ski)
}

func (r shipReader) RemoteSKIDisconnected(ski string) {
	r.device.RemoveRemoteDeviceConnection(ski)
	r.Instance.RemoteSKIDisconnected(nil, ski)
}

func (r shipReader) SetupRemoteDevice(ski string, writer shipapi.ShipConnectionDataWriterInterface) shipapi.ShipConnectionDataReaderInterface {
	return r.device.SetupRemoteDevice(ski, writer)
}

// The instance's browser has the full TXT record and endpoint for the view.
// Do not replace it with the hub's less detailed projection of the same data.
func (shipReader) VisibleRemoteServicesUpdated([]shipapi.RemoteService) {}

func (r shipReader) AllowWaitingForTrust(string) bool { return r.register }
