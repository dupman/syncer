/*
 * This file is part of the dupman/syncer project.
 *
 * (c) 2022. dupman <info@dupman.cloud>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * Written by Temuri Takalandze <me@abgeo.dev>
 */

package lib

import (
	"github.com/dupman/celery"
	"github.com/dupman/config"
)

// DupmanConfig represents config for dupman client.
type DupmanConfig struct {
	URL      string `mapstructure:"DUPMAN_URL"`
	Username string `mapstructure:"DUPMAN_USERNAME"`
	Password string `mapstructure:"DUPMAN_PASSWORD"`
}

// Config represents General config.
type Config struct {
	Celery celery.Config `mapstructure:",squash"`
	Dupman DupmanConfig  `mapstructure:",squash"`
}

// NewConfig creates a new Config.
func NewConfig() (conf Config) {
	config.Load(".", &conf)

	return conf
}
