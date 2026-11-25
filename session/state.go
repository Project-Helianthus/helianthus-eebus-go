package session

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/Project-Helianthus/helianthus-spine-go/model"
)

const stateFileName = "feature-pairs.json"

var errCorruptState = errors.New("feature-pair state cannot be read")

type storedFile struct {
	Pairs []storedPair `json:"pairs"`
}

type storedPair struct {
	Peer              string `json:"peer"`
	Role              Role   `json:"role"`
	ID                uint   `json:"id"`
	ClientDevice      string `json:"clientDevice"`
	ClientEntity      []uint `json:"clientEntity"`
	ClientFeature     uint   `json:"clientFeature"`
	ServerDevice      string `json:"serverDevice"`
	ServerEntity      []uint `json:"serverEntity"`
	ServerFeature     uint   `json:"serverFeature"`
	ServerFeatureType string `json:"serverFeatureType"`
}

func statePath(root string) string {
	return filepath.Join(filepath.Clean(root), stateFileName)
}

func loadState(root string) ([]storedPair, error) {
	path := statePath(root)
	buf, err := os.ReadFile(path) // #nosec G304 -- path is the instance state root plus the fixed file name
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []storedPair{}, nil
		}
		return nil, err
	}
	var file storedFile
	if err := json.Unmarshal(buf, &file); err != nil {
		return nil, errCorruptState
	}
	return file.Pairs, nil
}

func writeState(root string, pairs []storedPair) error {
	file := storedFile{Pairs: pairs}
	buf, err := json.Marshal(file)
	if err != nil {
		return err
	}
	buf = append(buf, '\n')
	path := statePath(root)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o600); err != nil { // #nosec G304 -- same instance state root
		return err
	}
	return os.Rename(tmp, path)
}

func storedFromEntry(peer string, role Role, id uint, pair Pair) storedPair {
	return storedPair{
		Peer:              peer,
		Role:              role,
		ID:                id,
		ClientDevice:      deviceString(pair.Client.Device),
		ClientEntity:      entityValues(pair.Client.Entity),
		ClientFeature:     featureValue(pair.Client.Feature),
		ServerDevice:      deviceString(pair.Server.Device),
		ServerEntity:      entityValues(pair.Server.Entity),
		ServerFeature:     featureValue(pair.Server.Feature),
		ServerFeatureType: string(pair.ServerFeatureType),
	}
}

func (s storedPair) pair() Pair {
	return Pair{
		Client:            addressFrom(s.ClientDevice, s.ClientEntity, s.ClientFeature),
		Server:            addressFrom(s.ServerDevice, s.ServerEntity, s.ServerFeature),
		ServerFeatureType: model.FeatureTypeType(s.ServerFeatureType),
	}
}

func addressFrom(device string, entity []uint, feature uint) model.FeatureAddressType {
	address := model.FeatureAddressType{
		Entity:  entityTypes(entity),
		Feature: featurePtr(feature),
	}
	if device != "" {
		value := model.AddressDeviceType(device)
		address.Device = &value
	}
	return address
}

func deviceString(device *model.AddressDeviceType) string {
	if device == nil {
		return ""
	}
	return string(*device)
}

func entityValues(entity []model.AddressEntityType) []uint {
	if len(entity) == 0 {
		return nil
	}
	out := make([]uint, len(entity))
	for i, value := range entity {
		out[i] = uint(value)
	}
	return out
}

func entityTypes(entity []uint) []model.AddressEntityType {
	if len(entity) == 0 {
		return nil
	}
	out := make([]model.AddressEntityType, len(entity))
	for i, value := range entity {
		out[i] = model.AddressEntityType(value)
	}
	return out
}

func featureValue(feature *model.AddressFeatureType) uint {
	if feature == nil {
		return 0
	}
	return uint(*feature)
}

func featurePtr(feature uint) *model.AddressFeatureType {
	value := model.AddressFeatureType(feature)
	return &value
}
