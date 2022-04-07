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

package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/dupman/celery"
	"github.com/dupman/encryptor"
	"github.com/dupman/sdk/dupman/session"
	"github.com/dupman/sdk/service/system"
	"github.com/dupman/sdk/service/system/model"
	"github.com/dupman/syncer/lib"
	"github.com/gocelery/gocelery"
)

func processWebsites(celeryClient *gocelery.CeleryClient, dupmanSession *session.Session) error {
	rsaEncryptor := encryptor.NewRSAEncryptor()

	err := rsaEncryptor.GenerateKeyPair()
	if err != nil {
		return fmt.Errorf("error generating key pair: %w", err)
	}

	publicKey, err := rsaEncryptor.PublicKey()
	if err != nil {
		return fmt.Errorf("error getting public key: %w", err)
	}

	systemService := system.New(dupmanSession)
	currentPage := 1
	totalPages := 1

	var wg sync.WaitGroup
	for currentPage <= totalPages {
		wg.Add(1)

		response, err := systemService.GetWebsites(publicKey, currentPage)
		if err != nil {
			log.Printf("Error getting websites: %s", err)
		}

		totalPages = response.Pagination.TotalPages
		log.Printf("Fetched websites from page %d of %d", currentPage, totalPages)
		currentPage++

		go func(websites []model.Website, rsaEncryptor *encryptor.RSAEncryptor, celeryClient *gocelery.CeleryClient) {
			defer wg.Done()

			for _, website := range websites {
				token, err := rsaEncryptor.Decrypt(website.Token)
				if err != nil {
					log.Printf("Failed to decrypt token for website %s", website.URL)

					continue
				}

				_, err = celeryClient.Delay("dupman.website.fetch", website.ID, website.URL, token)
				if err != nil {
					log.Printf("Failed to create job for website %s", website.URL)
				}
			}
		}(response.Websites, rsaEncryptor, celeryClient)
	}
	wg.Wait()

	return nil
}

func main() {
	conf := lib.NewConfig()

	// Create new Celery client.
	celeryClient, err := celery.NewClient(&conf.Celery)
	if err != nil {
		log.Fatalf("Failed to create celery client: %s", err)
	}

	// Create new Dupman client.
	dupmanSession, err := session.New(conf.Dupman.Username, conf.Dupman.Password, conf.Dupman.URL)
	if err != nil {
		log.Fatalf("Failed to create dupman session: %s", err)
	}

	err = processWebsites(celeryClient, dupmanSession)
	if err != nil {
		log.Fatalf("Failed to process websites: %s", err)
	}
}
