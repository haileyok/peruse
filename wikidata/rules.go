package wikidata

import (
	"context"

	"github.com/haileyok/photocopy/nervana"
)

func ShouldInclude(ctx context.Context, relevantEntities map[string]Entity, responseEntities []nervana.NervanaItem) bool {
	// filter down to the relevant entities
	var filteredEntities []nervana.NervanaItem
	for _, e := range responseEntities {
		_, exists := relevantEntities[e.EntityId]
		if !exists {
			continue
		}
		filteredEntities = append(filteredEntities, e)
	}

	if len(filteredEntities) == 0 {
		return false
	}

	// If an entity is a person and there is only a single entity, ignore it. Too many false positives
	for _, e := range filteredEntities {
		entity := relevantEntities[e.EntityId]
		if entity.InstanceOf == EntityIdHuman && len(filteredEntities) < 2 {
			return false
		}
	}

	return true
}
