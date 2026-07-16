package service

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/Project-Helianthus/helianthus-eebus-go/api"
	shipapi "github.com/Project-Helianthus/helianthus-ship-go/api"
	"github.com/Project-Helianthus/helianthus-ship-go/cert"
	"github.com/Project-Helianthus/helianthus-ship-go/hub"
	"github.com/Project-Helianthus/helianthus-ship-go/logging"
	"github.com/Project-Helianthus/helianthus-ship-go/mdns"
	spineapi "github.com/Project-Helianthus/helianthus-spine-go/api"
	"github.com/Project-Helianthus/helianthus-spine-go/model"
	"github.com/Project-Helianthus/helianthus-spine-go/spine"
)

// OutgoingAttemptBridgeConfiguration configures the optional attempt-aware SHIP bridge.
type OutgoingAttemptBridgeConfiguration struct {
	Gate shipapi.OutgoingAttemptGate
	Sink shipapi.OutgoingAttemptHubReaderInterface
}

type connectionsHubFactory func(
	shipapi.HubReaderInterface,
	shipapi.MdnsInterface,
	int,
	tls.Certificate,
	*shipapi.ServiceDetails,
) shipapi.HubInterface

func defaultConnectionsHubFactory(
	reader shipapi.HubReaderInterface,
	mdnsService shipapi.MdnsInterface,
	port int,
	certificate tls.Certificate,
	localService *shipapi.ServiceDetails,
) shipapi.HubInterface {
	return hub.NewHub(reader, mdnsService, port, certificate, localService)
}

// A service is the central element of an EEBUS service
// including its websocket server and a zeroconf service.
type Service struct {
	configuration *api.Configuration

	// The local service details
	localService *shipapi.ServiceDetails

	// Connection Registrations
	connectionsHub shipapi.HubInterface

	// The SPINE specific device definition
	spineLocalDevice spineapi.DeviceLocalInterface

	serviceHandler api.ServiceReaderInterface

	usecases []api.UseCaseInterface

	// defines wether a user interaction to accept pairing is possible
	isPairingPossible bool

	bridgeEnabled       bool
	outgoingAttemptGate shipapi.OutgoingAttemptGate
	outgoingAttemptSink shipapi.OutgoingAttemptHubReaderInterface

	connectionsHubFactory connectionsHubFactory

	startOnce    sync.Once
	shutdownOnce sync.Once
	lifecycleMux sync.Mutex

	mux sync.Mutex
}

// creates a new EEBUS service
func NewService(configuration *api.Configuration, serviceHandler api.ServiceReaderInterface) *Service {
	return &Service{
		configuration:         configuration,
		serviceHandler:        serviceHandler,
		connectionsHubFactory: defaultConnectionsHubFactory,
	}
}

// NewServiceWithOutgoingAttemptBridge creates a service with attempt-aware SHIP callbacks.
func NewServiceWithOutgoingAttemptBridge(
	configuration *api.Configuration,
	serviceHandler api.ServiceReaderInterface,
	bridge OutgoingAttemptBridgeConfiguration,
) *Service {
	service := NewService(configuration, serviceHandler)
	service.bridgeEnabled = true
	service.outgoingAttemptGate = bridge.Gate
	service.outgoingAttemptSink = bridge.Sink
	return service
}

var _ api.ServiceInterface = (*Service)(nil)

// Starts the service by initializeing mDNS and the server.
func (s *Service) Setup() error {
	s.lifecycleMux.Lock()
	defer s.lifecycleMux.Unlock()

	if s.connectionsHub != nil {
		return nil
	}
	if s.bridgeEnabled {
		if isNilInterface(s.outgoingAttemptGate) {
			return errors.New("missing outgoing attempt gate")
		}
		if isNilInterface(s.outgoingAttemptSink) {
			return errors.New("missing outgoing attempt sink")
		}
	}
	if s.connectionsHubFactory == nil {
		return errors.New("missing connections hub factory")
	}

	sd := s.configuration

	if len(sd.Certificate().Certificate) == 0 {
		return errors.New("missing certificate")
	}

	leaf, err := x509.ParseCertificate(sd.Certificate().Certificate[0])
	if err != nil {
		return err
	}

	ski, err := cert.SkiFromCertificate(leaf)
	if err != nil {
		return err
	}

	// Initialize the local service
	// The ShipID is defined in SHIP Spec 3. as
	//   Each SHIP node has a globally unique SHIP ID. The SHIP ID is used to uniquely identify a SHIP node,
	//   e.g. in its service discovery. This ID is present in the mDNS/DNS-SD local service discovery;
	// In SHIP 13.4.6.2 the accessMethods.id is defined as
	//   The originator's unique ID
	// I assume those two to mean the same.
	// TODO: clarify
	localService := shipapi.NewServiceDetails(ski)
	localService.SetShipID(sd.Identifier())
	localService.SetDeviceType(string(sd.DeviceType()))

	logging.Log().Info("Local SKI:", ski)

	vendor := sd.VendorCode()
	if vendor == "" {
		vendor = sd.DeviceBrand()
	}

	serial := sd.DeviceSerialNumber()
	if serial != "" {
		serial = fmt.Sprintf("-%s", serial)
	}

	// Create the SPINE device address, according to Protocol Specification 7.1.1.2
	var deviceAddress string
	vendorType := "i"
	if _, err := strconv.Atoi(vendor); err != nil {
		vendorType = "n"
	}
	deviceAddress = fmt.Sprintf("d:_%s:%s_%s%s", vendorType, vendor, sd.DeviceModel(), serial)

	if len(deviceAddress) > 256 {
		return fmt.Errorf("generated device address may not be longer than 256 characters: %s", deviceAddress)
	}

	// Create the local SPINE device
	spineLocalDevice := spine.NewDeviceLocal(
		sd.DeviceBrand(),
		sd.DeviceModel(),
		sd.DeviceSerialNumber(),
		sd.Identifier(),
		deviceAddress,
		sd.DeviceType(),
		sd.FeatureSet(),
	)

	// Create the device entities and add it to the SPINE device
	for _, entityType := range sd.EntityTypes() {
		entityAddressId := model.AddressEntityType(len(spineLocalDevice.Entities()))
		entityAddress := []model.AddressEntityType{entityAddressId}
		entity := spine.NewEntityLocal(spineLocalDevice, entityType, entityAddress, sd.HeartbeatTimeout())
		spineLocalDevice.AddEntity(entity)
	}

	// setup mDNS
	mdnsService := mdns.NewMDNS(
		localService.SKI(),
		sd.DeviceBrand(),
		sd.DeviceModel(),
		string(sd.DeviceType()),
		sd.Identifier(),
		sd.MdnsServiceName(),
		sd.Port(),
		sd.Interfaces(),
		sd.MdnsProviderSelection(),
	)

	// Setup connections hub with mDNS and websocket connection handling
	connectionsHub := s.connectionsHubFactory(
		s,
		mdnsService,
		s.configuration.Port(),
		s.configuration.Certificate(),
		localService,
	)
	if isNilInterface(connectionsHub) {
		return errors.New("connections hub factory returned nil")
	}
	if s.bridgeEnabled {
		setter, ok := connectionsHub.(shipapi.OutgoingAttemptGateSetter)
		if !ok || isNilInterface(setter) {
			return errors.New("connections hub does not support outgoing attempt gate installation")
		}
		if err := setter.SetOutgoingAttemptGate(s.outgoingAttemptGate); err != nil {
			return fmt.Errorf("install outgoing attempt gate: %w", err)
		}
	}

	s.localService = localService
	s.spineLocalDevice = spineLocalDevice
	s.connectionsHub = connectionsHub

	return nil
}

// Starts the service
func (s *Service) Start() {
	s.lifecycleMux.Lock()
	defer s.lifecycleMux.Unlock()

	if s.connectionsHub == nil {
		return
	}
	s.startOnce.Do(func() {
		s.connectionsHub.Start()
	})
}

// Shutdown all services and stop the server.
func (s *Service) Shutdown() {
	s.lifecycleMux.Lock()
	defer s.lifecycleMux.Unlock()

	if s.connectionsHub == nil {
		return
	}
	s.shutdownOnce.Do(func() {
		s.connectionsHub.Shutdown()
	})
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// add a use case to the service
func (s *Service) AddUseCase(useCase api.UseCaseInterface) {
	s.usecases = append(s.usecases, useCase)

	useCase.AddFeatures()
	useCase.AddUseCase()
}

func (s *Service) Configuration() *api.Configuration {
	return s.configuration
}

func (s *Service) LocalService() *shipapi.ServiceDetails {
	return s.localService
}

func (s *Service) LocalDevice() spineapi.DeviceLocalInterface {
	return s.spineLocalDevice
}

// Sets a custom logging implementation
// By default NoLogging is used, so no logs are printed
func (s *Service) SetLogging(logger logging.LoggingInterface) {
	if logger == nil {
		return
	}
	logging.SetLogging(logger)
}

// Get the current pairing details for a given SKI
func (s *Service) PairingDetailForSki(ski string) *shipapi.ConnectionStateDetail {
	return s.connectionsHub.PairingDetailForSki(ski)
}

// Returns the Service detail of a given remote SKI
func (s *Service) RemoteServiceForSKI(ski string) *shipapi.ServiceDetails {
	return s.connectionsHub.ServiceForSKI(ski)
}

func (s *Service) SetAutoAccept(value bool) {
	s.localService.SetAutoAccept(value)
	s.connectionsHub.SetAutoAccept(value)
}

func (s *Service) IsAutoAcceptEnabled() bool {
	return s.localService.AutoAccept()
}

// Sets the SKI as being paired
// and connect it if paired and not currently being connected
func (s *Service) RegisterRemoteSKI(ski string) {
	s.connectionsHub.RegisterRemoteSKI(ski)
}

// Sets the SKI as not being paired
// and disconnects it if connected
func (s *Service) UnregisterRemoteSKI(ski string) {
	s.connectionsHub.UnregisterRemoteSKI(ski)
}

// Close a connection to a remote SKI
func (s *Service) DisconnectSKI(ski string, reason string) {
	s.connectionsHub.DisconnectSKI(ski, reason)
}

// Cancels the pairing process for a SKI
func (s *Service) CancelPairingWithSKI(ski string) {
	s.connectionsHub.CancelPairingWithSKI(ski)
}

// Define wether the user is able to react to an incoming pairing request
//
// Call this with `true` e.g. if the user is currently using a web interface
// where an incoming request can be accepted or denied
//
// Default is set to false, meaning every incoming pairing request will be
// automatically denied
func (s *Service) UserIsAbleToApproveOrCancelPairingRequests(allow bool) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.isPairingPossible = allow
}
