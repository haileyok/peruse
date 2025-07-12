package wikidata

type Entity struct {
	Entity     string `json:"entity"`
	Property   string `json:"property"`
	InstanceOf string `json:"instanceOf"`
}

const (
	PropertyId = "Q5"

	EntityIdHuman = "Q809898"
)
