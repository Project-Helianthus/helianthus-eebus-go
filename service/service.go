package service

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/netip"
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

// ListenerPolicy defines the exact SHIP listener endpoint and discovery behavior.
type ListenerPolicy struct {
	ListenAddress    netip.AddrPort
	DiscoveryEnabled bool
}

// ServiceOptions composes optional service runtime policies.
type ServiceOptions struct {
	ListenerPolicy        *ListenerPolicy
	OutgoingAttemptBridge *OutgoingAttemptBridgeConfiguration
}

type connectionsHubFactory func(
	shipapi.HubReaderInterface,
	shipapi.MdnsInterface,
	int,
	tls.Certificate,
	*shipapi.ServiceDetails,
) shipapi.HubInterface

type scopedConnectionsHubFactory func(
	shipapi.HubReaderInterface,
	shipapi.MdnsInterface,
	int,
	tls.Certificate,
	*shipapi.ServiceDetails,
	ListenerPolicy,
) (shipapi.HubInterface, error)

type listenerPolicyHub interface {
	shipapi.HubInterface
	StartWithPolicy() error
}

type pairingRegistrationHub = shipapi.PairingRegistrationSetter

type lifecycleState uint8

const (
	lifecycleUninitialized lifecycleState = iota
	lifecycleSettingUp
	lifecycleReady
	lifecycleStarting
	lifecycleRunning
	lifecycleStopping
	lifecycleStopped
	lifecycleTerminal
)

func defaultConnectionsHubFactory(
	reader shipapi.HubReaderInterface,
	mdnsService shipapi.MdnsInterface,
	port int,
	certificate tls.Certificate,
	localService *shipapi.ServiceDetails,
) shipapi.HubInterface {
	return hub.NewHub(reader, mdnsService, port, certificate, localService)
}

func defaultScopedConnectionsHubFactory(
	reader shipapi.HubReaderInterface,
	mdnsService shipapi.MdnsInterface,
	port int,
	certificate tls.Certificate,
	localService *shipapi.ServiceDetails,
	policy ListenerPolicy,
) (shipapi.HubInterface, error) {
	return hub.NewHubWithListenerPolicy(
		reader,
		mdnsService,
		port,
		certificate,
		localService,
		shipapi.ListenerPolicy{
			ListenAddress:    policy.ListenAddress,
			DiscoveryEnabled: policy.DiscoveryEnabled,
		},
	)
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
	listenerPolicy      *ListenerPolicy

	connectionsHubFactory       connectionsHubFactory
	scopedConnectionsHubFactory scopedConnectionsHubFactory

	lifecycleMux sync.Mutex
	lifecycle    lifecycleState

	mux sync.Mutex
}

// creates a new EEBUS service
func NewService(configuration *api.Configuration, serviceHandler api.ServiceReaderInterface) *Service {
	return NewServiceWithOptions(configuration, serviceHandler, ServiceOptions{})
}

// NewServiceWithOutgoingAttemptBridge creates a service with attempt-aware SHIP callbacks.
func NewServiceWithOutgoingAttemptBridge(
	configuration *api.Configuration,
	serviceHandler api.ServiceReaderInterface,
	bridge OutgoingAttemptBridgeConfiguration,
) *Service {
	return NewServiceWithOptions(configuration, serviceHandler, ServiceOptions{
		OutgoingAttemptBridge: &bridge,
	})
}

// NewServiceWithOptions creates a service with optional runtime policies.
func NewServiceWithOptions(
	configuration *api.Configuration,
	serviceHandler api.ServiceReaderInterface,
	options ServiceOptions,
) *Service {
	service := &Service{
		configuration:               configuration,
		serviceHandler:              serviceHandler,
		connectionsHubFactory:       defaultConnectionsHubFactory,
		scopedConnectionsHubFactory: defaultScopedConnectionsHubFactory,
	}

	if options.ListenerPolicy != nil {
		policy := *options.ListenerPolicy
		service.listenerPolicy = &policy
	}
	if options.OutgoingAttemptBridge != nil {
		bridge := *options.OutgoingAttemptBridge
		service.bridgeEnabled = true
		service.outgoingAttemptGate = bridge.Gate
		service.outgoingAttemptSink = bridge.Sink
	}

	return service
}

var _ api.ServiceInterface = (*Service)(nil)

// Starts the service by initializeing mDNS and the server.
func (s *Service) Setup() error {
	s.lifecycleMux.Lock()
	switch s.lifecycle {
	case lifecycleSettingUp:
		s.lifecycleMux.Unlock()
		return errors.New("setup already in progress")
	case lifecycleReady, lifecycleStarting, lifecycleRunning, lifecycleStopping, lifecycleStopped, lifecycleTerminal:
		s.lifecycleMux.Unlock()
		return nil
	}
	s.lifecycle = lifecycleSettingUp
	s.lifecycleMux.Unlock()

	setupSucceeded := false
	defer func() {
		if setupSucceeded {
			return
		}
		s.lifecycleMux.Lock()
		if s.lifecycle == lifecycleSettingUp {
			s.lifecycle = lifecycleUninitialized
		}
		s.lifecycleMux.Unlock()
	}()

	if s.bridgeEnabled {
		if isNilInterface(s.outgoingAttemptGate) {
			return errors.New("missing outgoing attempt gate")
		}
		if isNilInterface(s.outgoingAttemptSink) {
			return errors.New("missing outgoing attempt sink")
		}
	}
	if s.listenerPolicy != nil {
		if s.scopedConnectionsHubFactory == nil {
			return errors.New("missing scoped connections hub factory")
		}
	} else if s.connectionsHubFactory == nil {
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

	// Setup connections hub with mDNS and websocket connection handling.
	var connectionsHub shipapi.HubInterface
	if s.listenerPolicy != nil {
		connectionsHub, err = s.scopedConnectionsHubFactory(
			s,
			mdnsService,
			s.configuration.Port(),
			s.configuration.Certificate(),
			localService,
			*s.listenerPolicy,
		)
		if err != nil {
			return fmt.Errorf("create scoped connections hub: %w", err)
		}
	} else {
		connectionsHub = s.connectionsHubFactory(
			s,
			mdnsService,
			s.configuration.Port(),
			s.configuration.Certificate(),
			localService,
		)
	}
	if isNilInterface(connectionsHub) {
		return errors.New("connections hub factory returned nil")
	}
	if s.listenerPolicy != nil {
		if _, ok := connectionsHub.(listenerPolicyHub); !ok {
			return errors.New("scoped connections hub does not support listener policy startup")
		}
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
	s.mux.Lock()
	pairingPossible := s.isPairingPossible
	s.mux.Unlock()
	pairingSetter, supportsPairingRegistration := connectionsHub.(pairingRegistrationHub)
	if pairingPossible && (!supportsPairingRegistration || isNilInterface(pairingSetter)) {
		return errors.New("connections hub does not support pairing registration")
	}
	if supportsPairingRegistration && !isNilInterface(pairingSetter) {
		if err := pairingSetter.SetPairingRegistration(pairingPossible); err != nil {
			return fmt.Errorf("set initial pairing registration: %w", err)
		}
	}

	s.lifecycleMux.Lock()
	s.localService = localService
	s.spineLocalDevice = spineLocalDevice
	s.connectionsHub = connectionsHub
	s.lifecycle = lifecycleReady
	setupSucceeded = true
	s.lifecycleMux.Unlock()

	return nil
}

// Starts the service
func (s *Service) Start() {
	if s.listenerPolicy != nil {
		if err := s.StartWithPolicy(); err != nil {
			logging.Log().Debug("error during listener policy service startup:", err)
		}
		return
	}

	s.lifecycleMux.Lock()
	if s.lifecycle != lifecycleReady {
		s.lifecycleMux.Unlock()
		return
	}
	hub := s.connectionsHub
	s.lifecycle = lifecycleStarting
	s.lifecycleMux.Unlock()

	hub.Start()

	s.lifecycleMux.Lock()
	if s.lifecycle == lifecycleStarting {
		s.lifecycle = lifecycleRunning
	}
	s.lifecycleMux.Unlock()
}

// StartWithPolicy synchronously starts a service configured with a listener policy.
func (s *Service) StartWithPolicy() error {
	if s.listenerPolicy == nil {
		return errors.New("service has no listener policy")
	}

	s.lifecycleMux.Lock()
	wasRunning := false
	switch s.lifecycle {
	case lifecycleReady:
		// Continue below.
	case lifecycleRunning:
		// Re-enter the hub so an asynchronously terminal listener is observable.
		wasRunning = true
	case lifecycleStarting:
		s.lifecycleMux.Unlock()
		return errors.New("listener policy service startup is already in progress")
	case lifecycleStopping, lifecycleStopped, lifecycleTerminal:
		s.lifecycleMux.Unlock()
		return errors.New("listener policy service lifecycle is terminal")
	default:
		s.lifecycleMux.Unlock()
		return errors.New("listener policy service is not ready")
	}

	hub := s.connectionsHub
	starter, ok := hub.(listenerPolicyHub)
	if !ok || isNilInterface(starter) {
		s.lifecycle = lifecycleTerminal
		s.lifecycleMux.Unlock()
		return errors.New("scoped connections hub does not support listener policy startup")
	}
	if !wasRunning {
		s.lifecycle = lifecycleStarting
	}
	s.lifecycleMux.Unlock()

	if err := starter.StartWithPolicy(); err != nil {
		s.lifecycleMux.Lock()
		claimCleanup := (!wasRunning && s.lifecycle == lifecycleStarting) ||
			(wasRunning && s.lifecycle == lifecycleRunning)
		if claimCleanup {
			s.lifecycle = lifecycleTerminal
		}
		s.lifecycleMux.Unlock()

		if claimCleanup {
			hub.Shutdown()
		}
		return err
	}
	if wasRunning {
		return nil
	}

	s.lifecycleMux.Lock()
	defer s.lifecycleMux.Unlock()
	if s.lifecycle == lifecycleStarting {
		s.lifecycle = lifecycleRunning
		return nil
	}
	return errors.New("listener policy service lifecycle is terminal")
}

// Shutdown all services and stop the server.
func (s *Service) Shutdown() {
	s.lifecycleMux.Lock()
	switch s.lifecycle {
	case lifecycleReady, lifecycleStarting, lifecycleRunning:
		// Continue below after making the terminal transition visible.
	default:
		s.lifecycleMux.Unlock()
		return
	}
	hub := s.connectionsHub
	s.lifecycle = lifecycleStopping
	s.lifecycleMux.Unlock()

	hub.Shutdown()

	s.lifecycleMux.Lock()
	s.lifecycle = lifecycleStopped
	s.lifecycleMux.Unlock()
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
	if err := s.SetPairingRegistration(allow); err != nil {
		logging.Log().Debug("set pairing registration failed", err)
	}
}

// SetPairingRegistration controls protected, user-mediated pairing
// availability without enabling automatic handshake acceptance.
func (s *Service) SetPairingRegistration(allow bool) error {
	s.mux.Lock()
	s.isPairingPossible = allow
	s.mux.Unlock()

	s.lifecycleMux.Lock()
	hub := s.connectionsHub
	s.lifecycleMux.Unlock()
	if isNilInterface(hub) {
		return nil
	}
	setter, ok := hub.(pairingRegistrationHub)
	if !ok || isNilInterface(setter) {
		return errors.New("connections hub does not support pairing registration")
	}
	return setter.SetPairingRegistration(allow)
}
