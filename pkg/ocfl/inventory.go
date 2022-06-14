package ocfl

import (
	"encoding/json"
	"fmt"
	"github.com/goph/emperror"
	"strconv"
	"strings"
	"time"
)

type OCFLTime time.Time

func (t *OCFLTime) MarshalJSON() ([]byte, error) {
	return json.Marshal((time.Time)(*t).Format(time.RFC3339))
}

func (t *OCFLTime) UnmarshalJSON(data []byte) error {
	tt, err := time.Parse(time.RFC3339, string(data))
	if err != nil {
		return emperror.Wrapf(err, "cannot parse %s", string(data))
	}
	*t = OCFLTime(tt)
	return nil
}

type User struct {
	Address string `json:"address"`
	Name    string `json:"name"`
}

type Version struct {
	Created OCFLTime            `json:"created"`
	Message string              `json:"message"`
	State   map[string][]string `json:"state"`
	User    User                `json:"user"`
}

type Inventory struct {
	Id               string                                  `json:"id"`
	Type             string                                  `json:"type"`
	DigestAlgorithm  DigestAlgorithm                         `json:"digestAlgorithm"`
	Head             string                                  `json:"head"`
	ContentDirectory string                                  `json:"contentDirectory,omitempty"`
	Manifest         map[string][]string                     `json:"manifest"`
	Versions         map[string]*Version                     `json:"versions"`
	Fixity           map[DigestAlgorithm]map[string][]string `json:"fixity,omitempty"`
}

func NewInventory(id, _type string, digestAlgorithm DigestAlgorithm) (*Inventory, error) {
	i := &Inventory{
		Id:               id,
		Type:             _type,
		DigestAlgorithm:  digestAlgorithm,
		Head:             "",
		ContentDirectory: "content",
		Manifest:         map[string][]string{},
		Versions:         map[string]*Version{},
		Fixity:           nil,
	}
	return i, nil
}

func (i *Inventory) NewVersion(msg, UserName, UserAddress string) error {
	if i.Head == "" {
		i.Head = "v1"
	} else {
		vStr := strings.TrimPrefix(strings.ToLower(i.Head), "v")
		v, err := strconv.Atoi(vStr)
		if err != nil {
			return emperror.Wrapf(err, "cannot determine head of Object - %s", vStr)
		}
		i.Head = fmt.Sprintf("v%d", v+1)
	}
	i.Versions[i.Head] = &Version{
		Created: OCFLTime(time.Now()),
		Message: msg,
		State:   map[string][]string{},
		User: User{
			Name:    UserName,
			Address: UserAddress,
		},
	}
	return nil
}
