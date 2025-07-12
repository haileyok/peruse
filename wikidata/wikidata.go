package wikidata

type Entity struct {
	Entity     string `json:"entity"`
	Property   string `json:"property"`
	InstanceOf string `json:"instanceOf"`
}

const (
	EntityIdHuman = "Q5"
)
