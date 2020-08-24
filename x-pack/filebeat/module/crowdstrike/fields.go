// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

// Code generated by beats/dev-tools/cmd/asset/asset.go - DO NOT EDIT.

package crowdstrike

import (
	"github.com/snappyflow/beats/v7/libbeat/asset"
)

func init() {
	if err := asset.SetFields("filebeat", "crowdstrike", asset.ModuleFieldsPri, AssetCrowdstrike); err != nil {
		panic(err)
	}
}

// AssetCrowdstrike returns asset data.
// This is the base64 encoded gzipped contents of module/crowdstrike.
func AssetCrowdstrike() string {
	return "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"
}