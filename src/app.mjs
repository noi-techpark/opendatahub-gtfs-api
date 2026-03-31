// SPDX-FileCopyrightText: NOI Techpark <digital@noi.bz.it>
//
// SPDX-License-Identifier: AGPL-3.0-or-later

import * as dotenv from 'dotenv'
import express, { Router } from 'express'
import yaml from 'js-yaml'
import fs from 'fs'
import got from 'got'
import pino from 'pino-http'
import NodeCache from 'node-cache'
import cors from 'cors'

import { getFile } from '@tpisto/ftp-any-get'

dotenv.config()
const datastoreConfigs = yaml.load(fs.readFileSync('datasets.yml')).datasets
const app = express()
const router = Router()
const port = 3000
const cache = new NodeCache()
const pinoHttp = pino({ level: process.env.LOG_LEVEL || 'info' })

app.use(pinoHttp)
app.use(cors())
app.set('trust proxy')

const FEED_TYPES = ['trip-updates', 'vehicle-positions', 'service-alerts']
const feedTypeToConfigKey = (feedType) => feedType.replace(/-/g, '_')
const configKeyToFeedType = (key) => key.replace(/_/g, '-')

const FORMAT_CONTENT_TYPES = {
  json: 'application/json',
  pb: 'application/x-protobuf'
}

function assembleMetadata (id, cfg) {
  const meta = {
    description: cfg.description,
    endpoint: `${process.env.API_BASE_URL}/v1/dataset/${id}/raw`,
    origin: cfg.origin,
    license: cfg.license,
    metadata: cfg.metadata
  }
  if (cfg.realtime) {
    meta.realtime = Object.fromEntries(
      Object.entries(cfg.realtime.feeds)
        .map(([type, _]) => [type, `${process.env.API_BASE_URL}/v1/realtime/${id}/${configKeyToFeedType(type)}`])
    )
  }
  return meta
}

function error (res, status, msg) {
  res.status(status)
  res.send({ error: msg })
}

function negotiateFormat (req) {
  const best = req.accepts(['application/json', 'application/x-protobuf'])
  if (best === 'application/x-protobuf') return 'pb'
  return 'json'
}

router.get('/dataset', (req, res) => {
  res.json(Object.fromEntries(
    Object.entries(datastoreConfigs)
      .map(([id, cfg]) => [id, assembleMetadata(id, cfg)])))
})

router.get('/dataset/:dataset', (req, res) => {
  const datasetId = req.params.dataset
  const config = datastoreConfigs[datasetId]
  if (!config) {
    return error(res, 404, `Dataset ${datasetId} not found!`)
  }
  res.json(assembleMetadata(datasetId, config))
})

// define source URL handlers for each supported protocol
const protocolHandlers = {
  http: (uri) => got.get(uri).buffer(),
  https: (uri) => got.get(uri).buffer(),
  ftp: (uri) => getFile(uri)
}

function fetchFromSource (uri) {
  const proto = uri.match(/^(\w+):.*/)[1].toLowerCase()
  const handler = protocolHandlers[proto]
  return handler(uri)
}

router.get('/dataset/:dataset/raw', async (req, res) => {
  const datasetId = req.params.dataset
  const datasetConfig = datastoreConfigs[datasetId]
  if (!datasetConfig) {
    return error(res, 404, `Dataset ${datasetId} not found!`)
  }

  let rawGTFS = cache.get(datasetId)
  if (!rawGTFS) {
    rawGTFS = await fetchFromSource(datasetConfig.source)
    cache.set(datasetId, rawGTFS, datasetConfig.cache_ttl)
  }

  res.writeHead(200, {
    'Content-Type': 'application/zip',
    'Content-disposition': 'attachment;filename=' + datasetId + '.zip',
    'Content-length': rawGTFS.length
  })
  res.end(rawGTFS)
})

// Realtime endpoints

router.get('/realtime', (req, res) => {
  const rtDatasets = Object.entries(datastoreConfigs)
    .filter(([_, cfg]) => cfg.realtime)
    .map(([id, cfg]) => [id, {
      dataset_id: id,
      static_dataset: `${process.env.API_BASE_URL}/v1/dataset/${id}`,
      feeds: Object.fromEntries(
        Object.entries(cfg.realtime.feeds)
          .map(([type, _]) => [type, `${process.env.API_BASE_URL}/v1/realtime/${id}/${configKeyToFeedType(type)}`])
      )
    }])
  res.json(Object.fromEntries(rtDatasets))
})

router.get('/realtime/:dataset/:feedType', async (req, res) => {
  const datasetId = req.params.dataset
  const feedType = req.params.feedType

  if (!FEED_TYPES.includes(feedType)) {
    return error(res, 404, `Unknown feed type ${feedType}!`)
  }

  const config = datastoreConfigs[datasetId]
  if (!config || !config.realtime) {
    return error(res, 404, `Realtime feeds for dataset ${datasetId} not found!`)
  }

  const configKey = feedTypeToConfigKey(feedType)
  const feedConfig = config.realtime.feeds[configKey]
  if (!feedConfig) {
    return error(res, 404, `Feed ${feedType} not available for dataset ${datasetId}!`)
  }

  const format = negotiateFormat(req)
  const sourceUrl = feedConfig.sources[format]
  if (!sourceUrl) {
    return error(res, 406, `Format ${format} not available for ${feedType} of dataset ${datasetId}!`)
  }

  const cacheKey = `rt:${datasetId}:${configKey}:${format}`
  const cacheTtl = feedConfig.cache_ttl || config.realtime.cache_ttl
  let data = cacheTtl ? cache.get(cacheKey) : undefined
  if (!data) {
    try {
      data = await fetchFromSource(sourceUrl)
    } catch (e) {
      req.log.error(e, `Failed to fetch ${feedType} for dataset ${datasetId}`)
      return error(res, 502, `Failed to fetch ${feedType} feed!`)
    }
    if (cacheTtl) cache.set(cacheKey, data, cacheTtl)
  }

  res.set('Content-Type', FORMAT_CONTENT_TYPES[format])
  res.send(data)
})

// Openapi stuff
const apiSpecUrl = `${process.env.API_BASE_URL}/v1/apispec`
const redirectSwagger = (req, res) => {
  res.redirect(`https://swagger.opendatahub.com/?url=${apiSpecUrl}`)
}
const openapiRouter = Router()
openapiRouter.get('/', redirectSwagger)
router.get('/', redirectSwagger)

// Load openapi spec in memory, replace the Server URL placeholder with our configured one
const apiSpecContent = fs.readFileSync('openapi3.yml', { encoding: 'utf8' })
  .replace('__API_BASE_URL__', process.env.API_BASE_URL)

router.get('/apispec', (req, res) => {
  res.set('Content-Type', 'application/yaml')
  res.send(apiSpecContent)
})

app.use('/', openapiRouter)
app.use('/v1/', router) // use v1 prefix for all URLs
app.listen(port, () => {
  console.log(`GTFS API listening on port ${port}`)
})
